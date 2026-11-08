"""Parte B.2 y D.1 — Consumidor del grupo `ingesta`: del stream de Redis a HBase."""
import argparse
import os
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
    TABLA_LECTURAS,
    TABLA_LECTURAS_TIPO,
    N_VIVO,
)
from claves import (
    clave_lecturas,
    clave_lecturas_tipo,
    columnas_lecturas,
    columnas_lecturas_tipo,
)


def escribir_lote_hbase(t_lecturas, t_tipo, entradas):
    """Escribe un lote de entradas del stream en `lecturas` y `lecturas_tipo` con batch."""
    with t_lecturas.batch() as b1, t_tipo.batch() as b2:
        for _, ev in entradas:
            b1.put(clave_lecturas(ev), columnas_lecturas(ev))
            b2.put(clave_lecturas_tipo(ev), columnas_lecturas_tipo(ev))


def reclamar_pendientes(r: redis.Redis, consumidor: str, t_lecturas, t_tipo, lote_tam: int = 200) -> int:
    """Recupera entradas pendientes en XPENDING (de este u otro consumidor caído) vía XAUTOCLAIM."""
    recuperadas = 0
    cursor = "0-0"
    while True:
        res = r.xautoclaim(STREAM, GRUPO_INGESTA, consumidor, min_idle_time=0, start_id=cursor, count=lote_tam)
        next_cursor, entradas = res[0], res[1]
        if entradas:
            escribir_lote_hbase(t_lecturas, t_tipo, entradas)
            ids = [msg_id for msg_id, _ in entradas]
            r.xack(STREAM, GRUPO_INGESTA, *ids)
            recuperadas += len(ids)
        if next_cursor == "0-0" or not entradas:
            break
        cursor = next_cursor
    return recuperadas


def consumir(
    consumidor: str = "ingesta-1",
    lote_tam: int = 200,
    esperados: int = N_VIVO,
    morir_en_lote: int | None = None,
):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        r.xgroup_create(STREAM, GRUPO_INGESTA, id="0", mkstream=True)
    except redis.ResponseError:
        pass

    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=60000)
    t_lecturas = conn.table(TABLA_LECTURAS)
    t_tipo = conn.table(TABLA_LECTURAS_TIPO)

    # 1. Primero recuperar cualquier pendiente dejado por un consumidor caído
    rec = reclamar_pendientes(r, consumidor, t_lecturas, t_tipo, lote_tam=lote_tam)
    if rec:
        print(f"[{consumidor}] Recuperadas y confirmadas {rec} entradas pendientes vía XAUTOCLAIM.")

    procesadas = rec
    lotes = 0
    vacias_seguidas = 0

    while True:
        resp = r.xreadgroup(GRUPO_INGESTA, consumidor, {STREAM: ">"}, count=lote_tam, block=1000)
        if not resp:
            vacias_seguidas += 1
            info_stream = r.xinfo_stream(STREAM)
            pend = r.xpending(STREAM, GRUPO_INGESTA)["pending"]
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
            ids = [msg_id for msg_id, _ in entradas]

            # 1º: put en HBase (las dos tablas)
            escribir_lote_hbase(t_lecturas, t_tipo, entradas)

            # Simulación de caída para Parte D.1: después del put y ANTES del XACK
            if morir_en_lote is not None and lotes == morir_en_lote:
                print(
                    f"[{consumidor}] 💀 Simulando caída (os._exit(1)) en lote {lotes} "
                    f"DESPUÉS del put de {len(ids)} eventos ({ids[0]} .. {ids[-1]}) y ANTES del XACK.",
                    flush=True,
                )
                os._exit(1)

            # 2º: XACK siempre después del put
            r.xack(STREAM, GRUPO_INGESTA, *ids)
            procesadas += len(ids)

            # Si el lote fue pequeño porque vamos al día con el stream, esperar 250 ms
            # para acumular un verdadero lote antes del siguiente viaje a HBase.
            if len(ids) < lote_tam // 2:
                time.sleep(0.25)

    conn.close()
    print(f"[{consumidor}] Finalizado: {procesadas:,} entradas procesadas en {lotes} lotes.")


def main():
    parser = argparse.ArgumentParser(description="Consumidor del grupo ingesta (Redis Stream -> HBase)")
    parser.add_argument("--consumidor", default="ingesta-1", help="Nombre del consumidor dentro del grupo")
    parser.add_argument("--lote", type=int, default=200, help="Tamaño de lote XREADGROUP")
    parser.add_argument("--esperados", type=int, default=N_VIVO, help="Total de eventos esperados en el stream")
    parser.add_argument("--morir-en-lote", type=int, default=None, help="Simular os._exit(1) tras el put del lote N")
    args = parser.parse_args()
    consumir(
        consumidor=args.consumidor,
        lote_tam=args.lote,
        esperados=args.esperados,
        morir_en_lote=args.morir_en_lote,
    )


if __name__ == "__main__":
    main()
