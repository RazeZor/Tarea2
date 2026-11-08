"""Parte B.1 — Creación de tablas pre-divididas y carga histórica en HBase."""
from concurrent.futures import ThreadPoolExecutor
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
    TABLA_LECTURAS_TIPO,
    generar_datos,
    hbase_shell,
)
from claves import (
    clave_lecturas,
    clave_lecturas_tipo,
    columnas_lecturas,
    columnas_lecturas_tipo,
)


def recrear_tablas():
    """Elimina (si existen) y crea las tablas `lecturas` y `lecturas_tipo` pre-divididas."""
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
    existentes = set(conn.tables())
    for t in (TABLA_LECTURAS, TABLA_LECTURAS_TIPO):
        if t.encode() in existentes:
            conn.delete_table(t, disable=True)
    conn.close()

    # Pre-split en 4 regiones por tabla, con VERSIONS => 3 para observar versiones en D.1
    comandos = """
create 'lecturas', {NAME => 'm', VERSIONS => 3}, SPLITS => ['COR', 'PEN', 'TAL']
create 'lecturas_tipo', {NAME => 'm', VERSIONS => 3}, SPLITS => ['flujo_vehicular', 'flujo_vehicular#2026-11-04T12:00', 'ocupacion_estacionamiento']
"""
    hbase_shell(comandos)


def _cargar_tabla(nombre_tabla: str, eventos_hist: list[dict], fn_clave, fn_cols, batch_size: int = 5000):
    t0 = time.perf_counter()
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=60000)
    tabla = conn.table(nombre_tabla)
    with tabla.batch(batch_size=batch_size) as b:
        for ev in eventos_hist:
            b.put(fn_clave(ev), fn_cols(ev))
    conn.close()
    dt = time.perf_counter() - t0
    return dt


def preparar_redis():
    """Limpia Redis y crea los dos consumer groups (`ingesta` y `tablero`) desde 0."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.flushall()
    r.xgroup_create(STREAM, GRUPO_INGESTA, id="0", mkstream=True)
    r.xgroup_create(STREAM, GRUPO_TABLERO, id="0", mkstream=True)


def main():
    print("=== Parte B.1: Carga histórica directo a HBase ===")
    t_gen = time.perf_counter()
    historico, _ = generar_datos()
    print(f"Generados {len(historico):,} eventos históricos en {time.perf_counter() - t_gen:.2f} s")
    print(f"  Rango: {historico[0]['timestamp']} -> {historico[-1]['timestamp']}")

    print("Recreando tablas pre-divididas (`lecturas` y `lecturas_tipo`)...")
    recrear_tablas()

    t_ini = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(_cargar_tabla, TABLA_LECTURAS, historico, clave_lecturas, columnas_lecturas)
        f2 = pool.submit(_cargar_tabla, TABLA_LECTURAS_TIPO, historico, clave_lecturas_tipo, columnas_lecturas_tipo)
        dt_lecturas = f1.result()
        dt_tipo = f2.result()
    dt_total = time.perf_counter() - t_ini

    print(f"  Tabla `lecturas`:      {len(historico):,} puts en {dt_lecturas:.2f} s ({len(historico)/dt_lecturas:,.0f} filas/s)")
    print(f"  Tabla `lecturas_tipo`: {len(historico):,} puts en {dt_tipo:.2f} s ({len(historico)/dt_tipo:,.0f} filas/s)")
    print(f"  Tiempo total paralelo: {dt_total:.2f} s ({2*len(historico)/dt_total:,.0f} puts/s combinados)")

    preparar_redis()
    print("Redis inicializado: stream `sensores:t2` y grupos `ingesta` y `tablero` creados.")


if __name__ == "__main__":
    main()
