"""Parte B.3 — Verificación integral del pipeline (Redis Stream + Tablero + HBase)."""
import sys
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
    CTRL_HIST_EVENTOS,
    CTRL_VIVO_EVENTOS,
    CTRL_TOTAL_EVENTOS,
    CTRL_TOTAL_REPETIDOS,
    CTRL_TOTAL_CLAVES,
    ESTACIONES,
    contar_filas_hbase,
)


def verificar() -> bool:
    print("\n" + "=" * 72)
    print("PARTE B.3 — VERIFICACIÓN DEL PIPELINE COMPLETO")
    print("=" * 72)

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=60000)

    # 1. Estado del Stream y Consumer Groups
    info_stream = r.xinfo_stream(STREAM)
    producidos_vivo = info_stream.get("entries-added", r.xlen(STREAM))
    xlen_actual = r.xlen(STREAM)

    grupos_info = {g["name"]: g for g in r.xinfo_groups(STREAM)}
    pend_ingesta = r.xpending(STREAM, GRUPO_INGESTA)["pending"]
    pend_tablero = r.xpending(STREAM, GRUPO_TABLERO)["pending"]
    leidos_ingesta = grupos_info.get(GRUPO_INGESTA, {}).get("entries-read", 0)
    leidos_tablero = grupos_info.get(GRUPO_TABLERO, {}).get("entries-read", 0)

    print("\n1. Stream de Redis (`sensores:t2`) y Consumer Groups:")
    print(f"   - Eventos producidos en vivo (entries-added): {producidos_vivo:,} (control: {CTRL_VIVO_EVENTOS:,})")
    print(f"   - Largo actual del stream (XLEN):             {xlen_actual:,}")
    print(f"   - Grupo `{GRUPO_INGESTA}`: procesadas={leidos_ingesta:,} | XPENDING={pend_ingesta} (esperado: 0)")
    print(f"   - Grupo `{GRUPO_TABLERO}`: procesadas={leidos_tablero:,} | XPENDING={pend_tablero} (esperado: 0)")

    # 2. Claves del Tablero en Redis
    ultimos = [est for est, _ in ESTACIONES if r.exists(f"ultimo:{est}")]
    ranking_n = r.zcard("ranking:flujo")
    ventanas = [est for est, t in ESTACIONES if t == "flujo_vehicular" and r.exists(f"ventana:{est}")]
    tipos = {t: r.scard(f"tipo:{t}") for t in ("flujo_vehicular", "ocupacion_estacionamiento", "calidad_aire")}

    print("\n2. Estructuras del Tablero en Redis:")
    print(f"   - Hashes `ultimo:<estacion>`:  {len(ultimos)}/16 estaciones")
    print(f"   - ZSET `ranking:flujo`:        {ranking_n}/8 estaciones de flujo vehicular")
    print(f"   - ZSETs `ventana:<estacion>`:  {len(ventanas)}/8 ventanas de 10 min activas")
    print(f"   - Sets `tipo:<tipo>` (catálogo): {tipos} (total={sum(tipos.values())}/16)")

    # 3. Conteo de filas en HBase
    print("\n3. Tablas de HBase (conteo con FirstKeyOnlyFilter() AND KeyOnlyFilter()):")
    filas_lecturas = contar_filas_hbase(conn, TABLA_LECTURAS)
    filas_tipo = contar_filas_hbase(conn, TABLA_LECTURAS_TIPO)
    conn.close()

    print(f"   - Filas en `{TABLA_LECTURAS}`:      {filas_lecturas:,} (control: {CTRL_TOTAL_CLAVES:,})")
    print(f"   - Filas en `{TABLA_LECTURAS_TIPO}`: {filas_tipo:,} (control: {CTRL_TOTAL_CLAVES:,})")

    # 4. Comparación contra números de control y explicación
    diff_eventos_claves = CTRL_TOTAL_EVENTOS - CTRL_TOTAL_CLAVES
    colisiones_ms = diff_eventos_claves - CTRL_TOTAL_REPETIDOS

    print("\n4. Explicación de cuadratura con los números de control:")
    print(f"   - Total eventos generados (histórico + vivo): {CTRL_HIST_EVENTOS:,} + {CTRL_VIVO_EVENTOS:,} = {CTRL_TOTAL_EVENTOS:,}")
    print(f"   - Eventos repetidos tal cual (P_REPETIDA=2%): {CTRL_TOTAL_REPETIDOS:,} (caen en la misma row key)")
    print(f"   - Choques de clave (misma estación y ms, distinto valor por lectura tardía): {colisiones_ms}")
    print(f"   - Claves distintas esperadas en HBase: {CTRL_TOTAL_EVENTOS:,} - {CTRL_TOTAL_REPETIDOS:,} - {colisiones_ms} = {CTRL_TOTAL_CLAVES:,}")

    ok = (
        producidos_vivo == CTRL_VIVO_EVENTOS
        and leidos_ingesta == CTRL_VIVO_EVENTOS
        and leidos_tablero == CTRL_VIVO_EVENTOS
        and pend_ingesta == 0
        and pend_tablero == 0
        and len(ultimos) == 16
        and ranking_n == 8
        and len(ventanas) == 8
        and sum(tipos.values()) == 16
        and filas_lecturas == CTRL_TOTAL_CLAVES
        and filas_tipo == CTRL_TOTAL_CLAVES
    )

    if ok:
        print("\n✅ VERIFICACIÓN EXITOSA: Todos los conteos calzan exactamente con los números de control.")
    else:
        print("\n❌ DIFERENCIAS DETECTADAS:")
        if producidos_vivo != CTRL_VIVO_EVENTOS:
            print(f"   - Eventos producidos ({producidos_vivo}) != {CTRL_VIVO_EVENTOS}")
        if pend_ingesta != 0 or pend_tablero != 0:
            print(f"   - XPENDING no es 0 (ingesta={pend_ingesta}, tablero={pend_tablero})")
        if filas_lecturas != CTRL_TOTAL_CLAVES or filas_tipo != CTRL_TOTAL_CLAVES:
            print(
                f"   - Filas HBase (`lecturas`={filas_lecturas}, `lecturas_tipo`={filas_tipo}) "
                f"!= {CTRL_TOTAL_CLAVES}"
            )
    return ok


if __name__ == "__main__":
    exito = verificar()
    sys.exit(0 if exito else 1)
