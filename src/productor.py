"""Parte B.2 — Productor de la hora en vivo hacia el stream `sensores:t2`."""
import argparse
import time
import redis

from config import (
    REDIS_HOST,
    REDIS_PORT,
    STREAM,
    MAXLEN,
    RITMO_VIVO,
    generar_datos,
)


def producir(limite: int | None = None, ritmo: float = RITMO_VIVO):
    _, en_vivo = generar_datos()
    if limite is not None:
        en_vivo = en_vivo[:limite]

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()

    print(f"Productor iniciado: {len(en_vivo):,} eventos -> stream `{STREAM}` (MAXLEN ~ {MAXLEN:,}, ritmo {ritmo*1000:.0f} ms/ev)")
    inicio = time.monotonic()
    for n, ev in enumerate(en_vivo):
        r.xadd(STREAM, ev, maxlen=MAXLEN, approximate=True)
        if ritmo > 0:
            espera = inicio + (n + 1) * ritmo - time.monotonic()
            if espera > 0:
                time.sleep(espera)

    duracion = time.monotonic() - inicio
    print(f"Productor finalizado: {len(en_vivo):,} eventos producidos en {duracion:.2f} s ({len(en_vivo)/duracion:.1f} ev/s)")


def main():
    parser = argparse.ArgumentParser(description="Productor del stream en vivo")
    parser.add_argument("--limite", type=int, default=None, help="Producir solo los primeros N eventos")
    parser.add_argument("--ritmo", type=float, default=RITMO_VIVO, help="Segundos entre eventos (default 0.025 = 20x)")
    args = parser.parse_args()
    producir(limite=args.limite, ritmo=args.ritmo)


if __name__ == "__main__":
    main()
