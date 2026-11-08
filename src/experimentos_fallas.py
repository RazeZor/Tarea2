"""Parte D — Experimentos de fallas:
D.1: El consumidor `ingesta` muere a mitad de lote (después del put y antes del XACK).
D.2: Redis se reinicia a mitad de camino (con persistencia AOF vs. sin persistencia).
"""
import argparse
import json
import subprocess
import sys
import time
import happybase
import redis

from config import (
    HBASE_HOST,
    HBASE_PORT,
    REDIS_HOST,
    REDIS_PORT,
    STREAM,
    GRUPO_INGESTA,
    GRUPO_TABLERO,
    TABLA_LECTURAS,
    ROOT_DIR,
    generar_datos,
)
from claves import clave_lecturas
from consumidor_ingesta import escribir_lote_hbase, reclamar_pendientes
from consumidor_tablero import LUA_ACTUALIZAR_TABLERO, procesar_entradas

RESULTADOS_DIR = ROOT_DIR / "resultados"


def inventario_redis(r: redis.Redis) -> dict:
    """Captura el estado de sobrevivencia en Redis antes/después de reiniciar."""
    existe_stream = bool(r.exists(STREAM))
    xlen = r.xlen(STREAM) if existe_stream else 0
    grupos = []
    if existe_stream:
        for g in r.xinfo_groups(STREAM):
            pend = r.xpending(STREAM, g["name"])["pending"]
            grupos.append({
                "name": g["name"],
                "consumers": g["consumers"],
                "pending": pend,
                "entries_read": g.get("entries-read", 0),
                "lag": g.get("lag", 0),
            })
    ultimos = len(r.keys("ultimo:*"))
    ventanas = len(r.keys("ventana:*"))
    tipos = len(r.keys("tipo:*"))
    ranking = r.zcard("ranking:flujo")
    return {
        "stream_existe": existe_stream,
        "xlen": xlen,
        "grupos": grupos,
        "claves_ultimo": ultimos,
        "claves_ventana": ventanas,
        "claves_tipo": tipos,
        "ranking_card": ranking,
    }


def poblar_mitad_camino(n_producir: int = 3000, n_procesar: int = 1500, n_dejar_pendientes: int = 100):
    """Limpia Redis, produce `n_producir` eventos, procesa `n_procesar` en ambos grupos
    y deja `n_dejar_pendientes` leídos pero sin XACK en `ingesta`.
    """
    _, en_vivo = generar_datos()
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.flushall()
    r.xgroup_create(STREAM, GRUPO_INGESTA, id="0", mkstream=True)
    r.xgroup_create(STREAM, GRUPO_TABLERO, id="0", mkstream=True)

    # Producir 3.000 eventos en pipeline para rapidez del test
    pipe = r.pipeline(transaction=False)
    for ev in en_vivo[:n_producir]:
        pipe.xadd(STREAM, ev, maxlen=10000, approximate=True)
    pipe.execute()

    # Grupo tablero procesa `n_procesar` eventos y hace XACK
    script_lua = r.register_script(LUA_ACTUALIZAR_TABLERO)
    resp_tab = r.xreadgroup(GRUPO_TABLERO, "tablero-1", {STREAM: ">"}, count=n_procesar)
    for _, entradas in resp_tab:
        procesar_entradas(r, script_lua, entradas)

    # Grupo ingesta procesa `n_procesar` con XACK, y luego lee `n_dejar_pendientes` SIN XACK
    resp_ing_ok = r.xreadgroup(GRUPO_INGESTA, "ingesta-1", {STREAM: ">"}, count=n_procesar)
    for _, entradas in resp_ing_ok:
        ids = [i for i, _ in entradas]
        r.xack(STREAM, GRUPO_INGESTA, *ids)

    r.xreadgroup(GRUPO_INGESTA, "ingesta-1", {STREAM: ">"}, count=n_dejar_pendientes)


def probar_config_redis(nombre: str, compose_args: list[str]) -> dict:
    print(f"\n--- D.2 Probando configuración: {nombre} ---")
    subprocess.run(["docker", "compose", *compose_args, "up", "-d", "--wait", "--force-recreate", "redis"], check=True, cwd=ROOT_DIR)
    poblar_mitad_camino(n_producir=3000, n_procesar=1500, n_dejar_pendientes=100)

    # Si es AOF everysec, esperar >1s para asegurar que el buffer AOF se haya sincronizado a disco
    time.sleep(1.5)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    antes = inventario_redis(r)
    print(f"  Estado ANTES de `docker compose restart redis`:  {json.dumps(antes, ensure_ascii=False)}")

    subprocess.run(["docker", "compose", *compose_args, "restart", "redis"], check=True, cwd=ROOT_DIR)
    subprocess.run(["docker", "compose", *compose_args, "up", "-d", "--wait", "redis"], check=True, cwd=ROOT_DIR)

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    despues = inventario_redis(r)
    print(f"  Estado DESPUÉS de `docker compose restart redis`: {json.dumps(despues, ensure_ascii=False)}")
    return {"configuracion": nombre, "antes": antes, "despues": despues}


def ejecutar_d2() -> dict:
    print("\n" + "=" * 72)
    print("PARTE D.2 — Experimento: Redis se reinicia a mitad de camino (3.000 eventos)")
    print("=" * 72)

    # 1. Configuración contraria: SIN persistencia (--save "" --appendonly no)
    res_sin_persist = probar_config_redis(
        "Contraria: Sin persistencia (--save '' --appendonly no)",
        ["-f", "docker-compose.yml", "-f", "docker-compose.sin-persistencia.yml"],
    )

    # 2. Configuración elegida en Parte A: CON persistencia AOF (appendonly yes + appendfsync everysec + volumen)
    res_con_aof = probar_config_redis(
        "Elegida (Parte A): AOF everysec con volumen pmd-redis",
        ["-f", "docker-compose.yml"],
    )

    return {"sin_persistencia": res_sin_persist, "con_aof_volumen": res_con_aof}


def inspeccionar_versiones_d1(fila_ejemplo: bytes) -> dict:
    """Muestra las versiones guardadas en la celda tras el reproceso del lote en D.1."""
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=60000)
    tabla = conn.table(TABLA_LECTURAS)
    versiones = tabla.cells(fila_ejemplo, b"m:valor", versions=3, include_timestamp=True)
    conn.close()
    lista_v = [{"valor": val.decode(), "hbase_cell_ts_ms": ts} for val, ts in versiones]
    print("\n--- D.1.4 Inspección de celda reprocesada en HBase (VERSIONS => 3) ---")
    print(f"  Row key inspeccionada: {fila_ejemplo.decode()}")
    print(f"  Versiones en `m:valor`: {lista_v}")
    print(f"  Lectura normal `row()[b'm:valor']`: {lista_v[0]['valor'] if lista_v else 'N/A'} (sin duplicar la fila)")
    return {"row_key": fila_ejemplo.decode(), "versiones": lista_v}


def main():
    parser = argparse.ArgumentParser(description="Experimentos de la Parte D")
    parser.add_argument("--solo-d2", action="store_true", help="Ejecutar únicamente D.2")
    args = parser.parse_args()

    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    res_d2 = ejecutar_d2()
    ruta = RESULTADOS_DIR / "experimento_d2.json"
    ruta.write_text(json.dumps(res_d2, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n✅ Resultado de D.2 guardado en {ruta}")


if __name__ == "__main__":
    main()
