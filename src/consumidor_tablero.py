"""Parte B.2 — Consumidor del grupo `tablero`: mantiene el estado 'de ahora' en Redis."""
import argparse
import redis

from config import (
    REDIS_HOST,
    REDIS_PORT,
    STREAM,
    GRUPO_TABLERO,
    VENTANA_MS,
    N_VIVO,
    ESTACIONES,
)
from claves import ts_a_ms

ESTACIONES_FLUJO = [e for e, t in ESTACIONES if t == "flujo_vehicular"]

# Script Lua atómico:
# 1. Agrega la estación al catálogo `tipo:<tipo>`.
# 2. Compara `ts_ms` de medición contra `HGET ultimo:<estacion> ts`:
#    solo actualiza `ultimo:<estacion>` y `ranking:flujo` si `ts_ms >= ts_prev`
#    (una lectura tardía nunca hace retroceder el tablero).
# 3. Si es de `flujo_vehicular`, inserta `ts_ms:valor` con score=`ts_ms` en `ventana:<estacion>`
#    y recorta las ventanas de flujo a los últimos 10 minutos de medición respecto al reloj máximo del stream.
LUA_ACTUALIZAR_TABLERO = """
local estacion = ARGV[1]
local tipo = ARGV[2]
local valor = ARGV[3]
local ts_ms = tonumber(ARGV[4])
local ventana_ms = tonumber(ARGV[5])

-- 1. Catálogo por tipo de sensor
redis.call('SADD', 'tipo:' .. tipo, estacion)

-- 2. Reloj máximo de medición del stream (solo para recortar la ventana de 10 min)
local max_stream = tonumber(redis.call('HGET', 'ultimo:' .. estacion, 'ts') or '-1')
for _, k in ipairs(KEYS) do
    local t = tonumber(redis.call('HGET', 'ultimo:' .. k, 'ts') or '-1')
    if t > max_stream then
        max_stream = t
    end
end
if ts_ms > max_stream then
    max_stream = ts_ms
end

-- 3. Último por medición: una lectura tardía (ts_ms < prev_ts) NO pisa `ultimo:` ni `ranking:flujo`
local clave_ultimo = 'ultimo:' .. estacion
local prev_ts = tonumber(redis.call('HGET', clave_ultimo, 'ts') or '-1')
if ts_ms >= prev_ts then
    redis.call('HSET', clave_ultimo, 'valor', valor, 'ts', tostring(ts_ms), 'tipo', tipo)
    if tipo == 'flujo_vehicular' then
        redis.call('ZADD', 'ranking:flujo', tonumber(valor), estacion)
    end
end

-- 4. Ventana móvil de 10 minutos de medición (solo flujo_vehicular)
if tipo == 'flujo_vehicular' then
    local clave_ventana = 'ventana:' .. estacion
    local miembro = tostring(ts_ms) .. ':' .. valor
    redis.call('ZADD', clave_ventana, ts_ms, miembro)
    local corte = max_stream - ventana_ms
    for _, est_flujo in ipairs(KEYS) do
        redis.call('ZREMRANGEBYSCORE', 'ventana:' .. est_flujo, '-inf', '(' .. tostring(corte))
    end
end
return 1
"""


def procesar_entradas(r: redis.Redis, script_lua, entradas):
    """Aplica el script Lua a un lote de entradas y luego confirma con XACK."""
    if not entradas:
        return 0
    pipe = r.pipeline(transaction=False)
    ids = []
    for msg_id, ev in entradas:
        ts_ms = ts_a_ms(ev["timestamp"])
        script_lua(
            keys=ESTACIONES_FLUJO,
            args=[ev["estacion_id"], ev["tipo_sensor"], str(ev["valor"]), str(ts_ms), str(VENTANA_MS)],
            client=pipe,
        )
        ids.append(msg_id)
    pipe.execute()
    r.xack(STREAM, GRUPO_TABLERO, *ids)
    return len(ids)


def consumir(consumidor: str = "tablero-1", lote_tam: int = 200, esperados: int = N_VIVO):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        r.xgroup_create(STREAM, GRUPO_TABLERO, id="0", mkstream=True)
    except redis.ResponseError:
        pass

    script_lua = r.register_script(LUA_ACTUALIZAR_TABLERO)

    # Recuperar pendientes si los hubiera
    procesadas = 0
    cursor = "0-0"
    while True:
        res = r.xautoclaim(STREAM, GRUPO_TABLERO, consumidor, min_idle_time=0, start_id=cursor, count=lote_tam)
        next_cursor, entradas = res[0], res[1]
        if entradas:
            procesadas += procesar_entradas(r, script_lua, entradas)
        if next_cursor == "0-0" or not entradas:
            break
        cursor = next_cursor

    lotes = 0
    vacias_seguidas = 0
    while True:
        resp = r.xreadgroup(GRUPO_TABLERO, consumidor, {STREAM: ">"}, count=lote_tam, block=1000)
        if not resp:
            vacias_seguidas += 1
            info_stream = r.xinfo_stream(STREAM)
            pend = r.xpending(STREAM, GRUPO_TABLERO)["pending"]
            if info_stream.get("entries-added", 0) >= esperados and pend == 0 and vacias_seguidas >= 2:
                break
            if vacias_seguidas >= 10:
                break
            continue

        vacias_seguidas = 0
        for _, entradas in resp:
            if not entradas:
                continue
            lotes += 1
            procesadas += procesar_entradas(r, script_lua, entradas)

    print(f"[{consumidor}] Finalizado: {procesadas:,} entradas procesadas en {lotes} lotes.")


def main():
    parser = argparse.ArgumentParser(description="Consumidor del grupo tablero (Redis Stream -> Estado en Redis)")
    parser.add_argument("--consumidor", default="tablero-1", help="Nombre del consumidor")
    parser.add_argument("--lote", type=int, default=200, help="Tamaño de lote XREADGROUP")
    parser.add_argument("--esperados", type=int, default=N_VIVO, help="Total de eventos esperados en el stream")
    args = parser.parse_args()
    consumir(consumidor=args.consumidor, lote_tam=args.lote, esperados=args.esperados)


if __name__ == "__main__":
    main()
