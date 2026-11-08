"""Parte C — Las 6 preguntas (con resultados, mediana de 5 corridas e interpretación)."""
from collections import defaultdict
import json
import math
from pathlib import Path
import time
import urllib.request
import happybase
import pandas as pd
import redis

from config import (
    HBASE_HOST,
    HBASE_PORT,
    REDIS_HOST,
    REDIS_PORT,
    STREAM,
    TABLA_LECTURAS,
    TABLA_LECTURAS_TIPO,
    VENTANA_MS,
    ESTACIONES,
    ROOT_DIR,
    en_contenedor_hbase,
    generar_datos,
    hbase_shell,
    medir,
)
from claves import (
    clave_lecturas,
    clave_ts_estacion,
    clave_salt_ts_estacion,
    columnas_lecturas,
    ts_a_ms,
)

URL_JMX = "http://localhost:16010/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions"
RESULTADOS_DIR = ROOT_DIR / "resultados"


def p1_ahora(r: redis.Redis, conn: happybase.Connection) -> dict:
    print("\n" + "=" * 72)
    print("PREGUNTA 1 — 'Ahora': Redis contra HBase")
    print("=" * 72)

    estaciones_ids = [e for e, _ in ESTACIONES]

    def consulta_redis():
        pipe = r.pipeline(transaction=False)
        for est in estaciones_ids:
            pipe.hgetall(f"ultimo:{est}")
        pipe.zrevrange("ranking:flujo", 0, 4, withscores=True)
        res = pipe.execute()
        ultimos = {est: res[i] for i, est in enumerate(estaciones_ids)}
        top5 = res[-1]
        return ultimos, top5

    def consulta_hbase():
        tabla = conn.table(TABLA_LECTURAS)
        ultimos = {}
        for est in estaciones_ids:
            # Con row key estacion#timestamp (ascendente), lo último está al final del prefijo:
            # usamos reverse=True, limit=1 sobre el prefijo de la estación.
            fila = next(tabla.scan(row_prefix=f"{est}#".encode(), reverse=True, limit=1))
            k, v = fila
            ts_iso = k.decode().split("#", 1)[1]
            ultimos[est] = {
                "valor": float(v[b"m:valor"].decode()),
                "ts_iso": ts_iso,
                "ts": ts_a_ms(ts_iso),
                "tipo": v[b"m:tipo"].decode(),
            }
        flujo = [
            (est, d["valor"])
            for est, d in ultimos.items()
            if d["tipo"] == "flujo_vehicular"
        ]
        flujo.sort(key=lambda x: x[1], reverse=True)
        return ultimos, flujo[:5]

    (ult_redis, top5_redis), t_redis, corridas_redis = medir(consulta_redis)
    (ult_hbase, top5_hbase), t_hbase, corridas_hbase = medir(consulta_hbase)

    print("\n1.1 Último valor de las 16 estaciones (medición):")
    filas_tabla = []
    for est in estaciones_ids:
        ur = ult_redis[est]
        uh = ult_hbase[est]
        filas_tabla.append({
            "estacion": est,
            "tipo": ur["tipo"],
            "valor_redis": float(ur["valor"]),
            "valor_hbase": uh["valor"],
            "ts_ms": int(ur["ts"]),
            "ts_iso": uh["ts_iso"],
        })
    df_ult = pd.DataFrame(filas_tabla)
    print(df_ult.to_string(index=False))

    print("\n1.2 Top 5 estaciones de flujo vehicular con más flujo:")
    print("   - Desde Redis (ZREVRANGE ranking:flujo 0 4):", top5_redis)
    print("   - Desde HBase (ordenado tras 16 reverse scans):", top5_hbase)

    # Identificar qué estación cambia entre último por medición y último por llegada
    entradas_stream = r.xrange(STREAM, min="-", max="+")
    ult_llegada = {}
    ult_medicion = {}
    for msg_id, ev in entradas_stream:
        est = ev["estacion_id"]
        ult_llegada[est] = (msg_id, ev)
        if est not in ult_medicion or ev["timestamp"] >= ult_medicion[est][1]["timestamp"]:
            ult_medicion[est] = (msg_id, ev)

    estacion_diferente = None
    for est in estaciones_ids:
        id_lleg, ev_lleg = ult_llegada[est]
        id_med, ev_med = ult_medicion[est]
        if ev_lleg["timestamp"] != ev_med["timestamp"] or ev_lleg["valor"] != ev_med["valor"]:
            estacion_diferente = {
                "estacion": est,
                "por_medicion": {"stream_id": id_med, **ev_med},
                "por_llegada": {"stream_id": id_lleg, **ev_lleg},
            }

    print("\n1.3 Estación que cambia según 'último por medición' vs 'último por llegada':")
    if estacion_diferente:
        e = estacion_diferente["estacion"]
        pm = estacion_diferente["por_medicion"]
        pl = estacion_diferente["por_llegada"]
        print(f"   - Estación identificada: {e}")
        print(f"   - Último por MEDICIÓN: valor={pm['valor']} | timestamp={pm['timestamp']} (ID stream: {pm['stream_id']})")
        print(f"   - Último por LLEGADA:  valor={pl['valor']} | timestamp={pl['timestamp']} (ID stream: {pl['stream_id']}, lectura tardía)")

    ratio = t_hbase / t_redis
    print("\n1.4 Tiempos medidos (mediana de 5 corridas, descartando la 1ª):")
    print(f"   - Redis (pipeline 16 HGETALL + 1 ZREVRANGE): {t_redis*1000:.3f} ms  (corridas: {[round(x*1000, 3) for x in corridas_redis]} ms)")
    print(f"   - HBase (16 reverse scans con limit=1):      {t_hbase*1000:.3f} ms  (corridas: {[round(x*1000, 3) for x in corridas_hbase]} ms)")
    print(f"   - Razón HBase / Redis:                       {ratio:.1f}× más lento en HBase")
    print("   - Interpretación: Un dashboard en vivo debe usar Redis porque mantiene el último estado y el ranking pre-ordenado en RAM (< 1 ms en un solo viaje de pipeline), mientras que HBase debe abrir 16 scanners reversos sobre bloques de disco/MemStore y ordenar en la aplicación.")

    return {
        "t_redis_ms": round(t_redis * 1000, 3),
        "t_hbase_ms": round(t_hbase * 1000, 3),
        "ratio_hbase_redis": round(ratio, 1),
        "top5": top5_redis,
        "estacion_diferente": estacion_diferente,
        "ultimos": filas_tabla,
    }


def p2_ventana_10min(r: redis.Redis) -> dict:
    print("\n" + "=" * 72)
    print("PREGUNTA 2 — Ventana de 10 minutos (Redis)")
    print("=" * 72)

    estaciones_ids = [e for e, _ in ESTACIONES]
    estaciones_flujo = sorted(r.smembers("tipo:flujo_vehicular"))

    def consulta_ventana():
        pipe_ts = r.pipeline(transaction=False)
        for est in estaciones_ids:
            pipe_ts.hget(f"ultimo:{est}", "ts")
        ancla = max(int(ts) for ts in pipe_ts.execute() if ts is not None)
        corte = ancla - VENTANA_MS

        pipe_v = r.pipeline(transaction=False)
        for est in estaciones_flujo:
            pipe_v.zrangebyscore(f"ventana:{est}", corte, ancla)
        listas = pipe_v.execute()

        promedios = {}
        for est, miembros in zip(estaciones_flujo, listas):
            valores = [float(m.split(":", 1)[1]) for m in miembros]
            promedios[est] = {
                "lecturas": len(valores),
                "promedio": round(sum(valores) / len(valores), 2) if valores else 0.0,
            }
        return ancla, promedios

    (ancla, promedios), t_med, corridas = medir(consulta_ventana)

    # Medición de MEMORY USAGE en las ventanas recortadas vs sin recortar
    mem_recortada = {}
    card_recortada = {}
    for est in estaciones_flujo:
        mem_recortada[est] = r.memory_usage(f"ventana:{est}") or 0
        card_recortada[est] = r.zcard(f"ventana:{est}")

    # Simular en una clave temporal qué pasa si NO se recorta durante toda la hora en vivo (y los 5 días)
    historico, en_vivo = generar_datos()
    clave_tmp_hora = "tmp:ventana_sin_recorte_1h:CC-001"
    clave_tmp_5d = "tmp:ventana_sin_recorte_5d:CC-001"
    r.delete(clave_tmp_hora, clave_tmp_5d)

    mapping_1h = {
        f"{ts_a_ms(ev['timestamp'])}:{ev['valor']}": ts_a_ms(ev["timestamp"])
        for ev in en_vivo if ev["estacion_id"] == "CC-001"
    }
    r.zadd(clave_tmp_hora, mapping_1h)
    mem_1h = r.memory_usage(clave_tmp_hora) or 0
    card_1h = r.zcard(clave_tmp_hora)

    mapping_5d = {
        f"{ts_a_ms(ev['timestamp'])}:{ev['valor']}": ts_a_ms(ev["timestamp"])
        for ev in (historico + en_vivo) if ev["estacion_id"] == "CC-001"
    }
    # Insertar en lotes para no saturar un comando
    items_5d = list(mapping_5d.items())
    for i in range(0, len(items_5d), 5000):
        r.zadd(clave_tmp_5d, dict(items_5d[i:i + 5000]))
    mem_5d = r.memory_usage(clave_tmp_5d) or 0
    card_5d = r.zcard(clave_tmp_5d)
    r.delete(clave_tmp_hora, clave_tmp_5d)

    print(f"\n2.1 Ancla (medición más reciente del stream): {ancla} ms (rango [{ancla - VENTANA_MS}, {ancla}])")
    filas = []
    for est in estaciones_flujo:
        filas.append({
            "estacion": est,
            "lecturas_10m": promedios[est]["lecturas"],
            "promedio_flujo": promedios[est]["promedio"],
            "memory_usage_bytes": mem_recortada[est],
        })
    print(pd.DataFrame(filas).to_string(index=False))

    total_mem_10m = sum(mem_recortada.values())
    print("\n2.2 Uso de memoria (MEMORY USAGE) — Con recorte vs. Sin recorte:")
    print(f"   - CC-001 recortada a 10 min ({card_recortada['CC-001']} entradas):     {mem_recortada['CC-001']:,} bytes (~{mem_recortada['CC-001']/card_recortada['CC-001']:.1f} B/entrada)")
    print(f"   - Total 8 estaciones recortadas a 10 min:          {total_mem_10m:,} bytes ({total_mem_10m/1024:.1f} KB)")
    print(f"   - CC-001 SIN recortar en 1 hora ({card_1h:,} entradas):    {mem_1h:,} bytes ({mem_1h/1024:.1f} KB)")
    print(f"   - CC-001 SIN recortar en 5 días ({card_5d:,} entradas):  {mem_5d:,} bytes ({mem_5d/(1024*1024):.2f} MB -> ~{8*mem_5d/(1024*1024):.1f} MB las 8 estaciones)")

    print(f"\n2.3 Tiempo medido (mediana de 5 corridas): {t_med*1000:.3f} ms (corridas: {[round(x*1000, 3) for x in corridas]} ms)")
    print("   - Interpretación: El Sorted Set con score=timestamp ordena lecturas tardías según su reloj de medición y permite acotar exactamente 10 minutos con ZREMRANGEBYSCORE/ZRANGEBYSCORE; sin ese recorte, la estructura crece linealmente (~87 B/entrada) hasta agotar la RAM de Redis.")

    return {
        "ancla_ms": ancla,
        "t_redis_ms": round(t_med * 1000, 3),
        "estaciones": filas,
        "mem_total_8_est_10m_bytes": total_mem_10m,
        "mem_cc001_10m_bytes": mem_recortada["CC-001"],
        "mem_cc001_1h_bytes": mem_1h,
        "mem_cc001_5d_bytes": mem_5d,
        "card_cc001_5d": card_5d,
    }


def p3_rango_hbase(conn: happybase.Connection) -> dict:
    print("\n" + "=" * 72)
    print("PREGUNTA 3 — Un rango en HBase: Scan por rango vs. Tabla entera + filtro")
    print("=" * 72)

    tabla = conn.table(TABLA_LECTURAS)
    row_ini = b"CC-001#2026-11-04T08:00"
    row_fin = b"CC-001#2026-11-04T10:00"
    filtro_regex = "RowFilter(=, 'regexstring:^CC-001#2026-11-04T0[89]:')"

    def procesar_filas(iter_filas):
        cuartos = defaultdict(list)
        total = 0
        for k, v in iter_filas:
            total += 1
            ts_str = k.decode().split("#", 1)[1]
            hh = int(ts_str[11:13])
            mm = int(ts_str[14:16])
            q = (mm // 15) * 15
            etiqueta = f"{hh:02d}:{q:02d}-{hh:02d}:{q+15:02d}" if q < 45 else f"{hh:02d}:45-{hh+1:02d}:00"
            cuartos[etiqueta].append(float(v[b"m:valor"].decode()))
        resumen = {
            et: {"lecturas": len(vals), "promedio": round(sum(vals) / len(vals), 2)}
            for et, vals in sorted(cuartos.items())
        }
        return total, resumen

    def consulta_rango():
        return procesar_filas(tabla.scan(row_start=row_ini, row_stop=row_fin, batch_size=1000))

    def consulta_filtro_completo():
        return procesar_filas(tabla.scan(filter=filtro_regex, batch_size=5000))

    (n_rango, cuartos_rango), t_rango, corridas_rango = medir(consulta_rango)
    (n_filtro, _), t_filtro, corridas_filtro = medir(consulta_filtro_completo)

    assert n_rango == n_filtro, f"Discrepancia: rango={n_rango} vs filtro={n_filtro}"

    print(f"\n3.1 Total lecturas de CC-001 el 04-nov [08:00, 10:00): {n_rango}")
    df_q = pd.DataFrame([
        {"cuarto_de_hora": k, "lecturas": v["lecturas"], "promedio_flujo": v["promedio"]}
        for k, v in cuartos_rango.items()
    ])
    print(df_q.to_string(index=False))

    factor = t_filtro / t_rango
    factor_5_anios = (5 * 365) / 5  # 365x más datos que 5 días
    t_est_5a_s = t_filtro * factor_5_anios

    print("\n3.2 Comparación de tiempos (mediana de 5 corridas):")
    print(f"   - Scan por rango [row_start, row_stop): {t_rango*1000:.2f} ms  (corridas: {[round(x*1000, 2) for x in corridas_rango]} ms)")
    print(f"   - Tabla entera + RowFilter en servidor: {t_filtro*1000:.2f} ms ({t_filtro:.3f} s) (corridas: {[round(x, 3) for x in corridas_filtro]} s)")
    print(f"   - Factor de lentitud:                   {factor:,.1f}× más lento el recorrido completo")
    print(f"   - Extrapolación a 5 años (×365 datos):  el scan por rango se mantiene en ~{t_rango*1000:.1f} ms (O(k), salta directo al bloque de CC-001#2026-11-04), mientras que el scan completo crecería linealmente O(N) a ~{t_est_5a_s/60:.1f} min (~{t_est_5a_s:.0f} s).")

    return {
        "total_lecturas": n_rango,
        "cuartos": cuartos_rango,
        "t_rango_ms": round(t_rango * 1000, 2),
        "t_filtro_ms": round(t_filtro * 1000, 2),
        "t_filtro_s": round(t_filtro, 3),
        "factor_lentitud": round(factor, 1),
        "extrapolacion_5a_min": round(t_est_5a_s / 60, 1),
    }


def p4_dos_patrones(r: redis.Redis, conn: happybase.Connection) -> dict:
    print("\n" + "=" * 72)
    print("PREGUNTA 4 — Dos patrones de acceso: Redis catálogo + HBase (`lecturas` vs `lecturas_tipo`)")
    print("=" * 72)

    t_lect = conn.table(TABLA_LECTURAS)
    t_tipo = conn.table(TABLA_LECTURAS_TIPO)

    def resumir(por_estacion):
        salida = {}
        for est, lista in sorted(por_estacion.items()):
            # Ordenar por timestamp ascendente para garantizar que el primer máximo sea el primero cronológico
            lista.sort(key=lambda x: x[0])
            valores = [v for _, v in lista]
            promedio = round(sum(valores) / len(valores), 2)
            max_val = max(valores)
            hora_max = next(ts for ts, v in lista if v == max_val)
            salida[est] = {
                "lecturas": len(valores),
                "promedio_pm25": promedio,
                "max_pm25": max_val,
                "hora_primer_max": hora_max,
            }
        return salida

    def patron_a_lecturas():
        estaciones_aire = sorted(r.smembers("tipo:calidad_aire"))
        por_est = defaultdict(list)
        for est in estaciones_aire:
            ini = f"{est}#2026-11-05T07:00".encode()
            fin = f"{est}#2026-11-05T09:00".encode()
            for k, v in t_lect.scan(row_start=ini, row_stop=fin, batch_size=1000):
                ts = k.decode().split("#", 1)[1]
                por_est[est].append((ts, float(v[b"m:valor"].decode())))
        return resumir(por_est)

    def patron_b_lecturas_tipo():
        por_est = defaultdict(list)
        ini = b"calidad_aire#2026-11-05T07:00"
        fin = b"calidad_aire#2026-11-05T09:00"
        for k, v in t_tipo.scan(row_start=ini, row_stop=fin, batch_size=2000):
            partes = k.decode().split("#")
            ts, est = partes[1], partes[2]
            por_est[est].append((ts, float(v[b"m:valor"].decode())))
        return resumir(por_est)

    res_a, t_a, corridas_a = medir(patron_a_lecturas)
    res_b, t_b, corridas_b = medir(patron_b_lecturas_tipo)

    assert res_a == res_b, f"Resultados distintos entre A y B: {res_a} != {res_b}"

    print("\n4.1 Resultados PM2.5 (05-nov [07:00, 09:00)) para estaciones de calidad del aire:")
    df_res = pd.DataFrame([{"estacion": k, **v} for k, v in res_a.items()])
    print(df_res.to_string(index=False))

    # Forzar flush para medir el espacio real en disco de ambas tablas en HBase
    hbase_shell("flush 'lecturas'\nflush 'lecturas_tipo'\n")
    du_out = en_contenedor_hbase(
        "du -sh /data/hbase/data/default/lecturas /data/hbase/data/default/lecturas_tipo"
    )

    print("\n4.2 Comparación de tiempos y costo de mantener la segunda tabla:")
    print(f"   - a) `lecturas` (SMEMBERS + 4 scans por estación): {t_a*1000:.2f} ms (corridas: {[round(x*1000, 2) for x in corridas_a]} ms)")
    print(f"   - b) `lecturas_tipo` (1 solo scan por tipo):       {t_b*1000:.2f} ms (corridas: {[round(x*1000, 2) for x in corridas_b]} ms)")
    print(f"   - Espacio en disco (HFiles tras flush):\n{du_out}")
    print("   - Interpretación: Con solo 4 estaciones de calidad del aire en un HBase standalone local, hacer 4 scans cortos en `lecturas` y 1 scan continuo en `lecturas_tipo` tardan órdenes similares (pocas decenas de ms), pero tener `lecturas_tipo` duplica las escrituras (×2 puts) y el espacio en disco; si la 2ª escritura falla antes del XACK, el consumidor reintentará el lote completo y la idempotencia de la row key corregirá la tabla faltante sin duplicar la primera. No vale la pena mantener `lecturas_tipo` cuando el catálogo tiene pocas estaciones por tipo o las consultas por tipo son infrecuentes frente al volumen de ingesta.")

    return {
        "resultados": res_a,
        "t_patron_a_ms": round(t_a * 1000, 2),
        "t_patron_b_ms": round(t_b * 1000, 2),
        "espacio_disco": du_out,
    }


def _escrituras_por_region(conn: happybase.Connection, tabla: str) -> dict[str, int]:
    """Lee writeRequestCount por región de `tabla` desde el endpoint JMX de HBase."""
    m = json.load(urllib.request.urlopen(URL_JMX))["beans"][0]
    conteos = {}
    for r in conn.table(tabla).regions():
        inicio = r["start_key"].decode() or "(inicio)"
        reg_hash = r["name"].decode().split(".")[-2]
        metric_key = f"Namespace_default_table_{tabla}_region_{reg_hash}_metric_writeRequestCount"
        conteos[inicio] = m.get(metric_key, 0)
    return conteos


def p5_hotspotting_salting(conn: happybase.Connection) -> dict:
    print("\n" + "=" * 72)
    print("PREGUNTA 5 — Hotspotting y salting en HBase (3 diseños de row key)")
    print("=" * 72)

    _, en_vivo = generar_datos()
    estaciones_ids = [e for e, _ in ESTACIONES]

    tablas_p5 = ("p5_est_ts", "p5_ts_est", "p5_salt_ts_est")
    existentes = set(conn.tables())
    for t in tablas_p5:
        if t.encode() in existentes:
            conn.delete_table(t, disable=True)

    # Tres tablas con 4 regiones cada una:
    # 1. p5_est_ts:      estacion#ts       -> pre-dividida por prefijo de estación
    # 2. p5_ts_est:      ts#estacion       -> pre-dividida por día (los 5 días + último corte 2026-11-06)
    # 3. p5_salt_ts_est: salt#ts#estacion  -> pre-dividida por salt (0, 1, 2, 3)
    hbase_shell("""
create 'p5_est_ts', 'm', SPLITS => ['COR', 'PEN', 'TAL']
create 'p5_ts_est', 'm', SPLITS => ['2026-11-03', '2026-11-05', '2026-11-06']
create 'p5_salt_ts_est', 'm', SPLITS => ['1', '2', '3']
""")

    print("Esperando 12 s para estabilizar contadores JMX iniciales...")
    time.sleep(12)
    antes = {t: _escrituras_por_region(conn, t) for t in tablas_p5}

    # Escribir la hora en vivo (7.200 eventos) en cada una de las 3 tablas
    for nombre, fn_clave in (
        ("p5_est_ts", clave_lecturas),
        ("p5_ts_est", clave_ts_estacion),
        ("p5_salt_ts_est", clave_salt_ts_estacion),
    ):
        with conn.table(nombre).batch(batch_size=2000) as b:
            for ev in en_vivo:
                b.put(fn_clave(ev), columnas_lecturas(ev))

    print("Esperando 15 s para publicación de métricas JMX de HBase...")
    time.sleep(15)
    despues = {t: _escrituras_por_region(conn, t) for t in tablas_p5}

    deltas = {}
    for t in tablas_p5:
        deltas[t] = {reg: despues[t][reg] - antes[t][reg] for reg in antes[t]}

    print("\n5.1 Escrituras recibidas por cada región durante la hora en vivo (JMX writeRequestCount):")
    filas_jmx = []
    nombres_diseno = {
        "p5_est_ts": "1) estacion#ts",
        "p5_ts_est": "2) ts#estacion (por día)",
        "p5_salt_ts_est": "3) salt#ts#estacion (mod 4)",
    }
    for t in tablas_p5:
        regs = list(deltas[t].items())
        fila = {"Diseño": nombres_diseno[t]}
        for idx, (rk, cnt) in enumerate(regs, 1):
            fila[f"Región {idx}"] = f"{cnt:,} (start='{rk}')"
        fila["Total"] = sum(cnt for _, cnt in regs)
        filas_jmx.append(fila)
    print(pd.DataFrame(filas_jmx).to_string(index=False))

    # Consultas de lectura: "todas las lecturas de todas las estaciones entre 00:10 y 00:20 del 07-nov"
    t1_est = conn.table("p5_est_ts")
    t2_ts = conn.table("p5_ts_est")
    t3_salt = conn.table("p5_salt_ts_est")

    def leer_16_rangos():
        n = 0
        for est in estaciones_ids:
            ini = f"{est}#2026-11-07T00:10".encode()
            fin = f"{est}#2026-11-07T00:20".encode()
            for _ in t1_est.scan(row_start=ini, row_stop=fin, batch_size=1000):
                n += 1
        return n

    def leer_1_rango():
        ini = b"2026-11-07T00:10"
        fin = b"2026-11-07T00:20"
        return sum(1 for _ in t2_ts.scan(row_start=ini, row_stop=fin, batch_size=2000))

    def leer_4_rangos():
        n = 0
        for s in range(4):
            ini = f"{s}#2026-11-07T00:10".encode()
            fin = f"{s}#2026-11-07T00:20".encode()
            for _ in t3_salt.scan(row_start=ini, row_stop=fin, batch_size=1000):
                n += 1
        return n

    n1, med_16r, corr_16r = medir(leer_16_rangos)
    n2, med_1r, corr_1r = medir(leer_1_rango)
    n3, med_4r, corr_4r = medir(leer_4_rangos)

    assert n1 == n2 == n3, f"Conteos distintos en P5: {n1}, {n2}, {n3}"

    print(f"\n5.2 Tiempo de consulta 'todas las estaciones el 07-nov [00:10, 00:20)' ({n1:,} filas):")
    print(f"   - Diseño 1 (`estacion#ts`, 16 rangos):      {med_16r*1000:.2f} ms (corridas: {[round(x*1000, 2) for x in corr_16r]} ms)")
    print(f"   - Diseño 2 (`ts#estacion`, 1 rango):        {med_1r*1000:.2f} ms (corridas: {[round(x*1000, 2) for x in corr_1r]} ms)")
    print(f"   - Diseño 3 (`salt#ts#estacion`, 4 rangos):  {med_4r*1000:.2f} ms (corridas: {[round(x*1000, 2) for x in corr_4r]} ms)")
    print("   - Interpretación: `ts#estacion` sufre hotspotting total (7.200 escrituras en la última región y 0 en las demás), mientras que `estacion#ts` y `salt#ts#estacion` reparten la carga en las 4 regiones; elegiríamos `estacion#ts` cuando las consultas filtran por estación (y hay pocas estaciones), pero cambiaríamos a `salt#ts#estacion` si hubiera miles de estaciones y la consulta principal fuera barrer ventanas globales de tiempo (4 rangos fijos en vez de miles).")

    return {
        "filas_rango_10m": n1,
        "jmx_deltas": deltas,
        "t_16_rangos_ms": round(med_16r * 1000, 2),
        "t_1_rango_ms": round(med_1r * 1000, 2),
        "t_4_rangos_ms": round(med_4r * 1000, 2),
    }


def p6_propia(r: redis.Redis, conn: happybase.Connection) -> dict:
    print("\n" + "=" * 72)
    print("PREGUNTA 6 — La propia: Detección de anomalías (Redis presente + HBase historia)")
    print("=" * 72)
    print("Pregunta formulada: 'Para las 5 estaciones con mayor flujo vehicular AHORA MISMO")
    print("(ranking en vivo de Redis), ¿cuánto se desvía su promedio de los últimos 10 minutos")
    print("(ventana móvil en Redis, 00:50–01:00 del 07-nov) respecto a su comportamiento")
    print("histórico en esa misma franja horaria (00:50–01:00 de los 5 días previos en HBase)?'")

    t_lect = conn.table(TABLA_LECTURAS)
    dias_hist = ["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06"]
    estaciones_ids = [e for e, _ in ESTACIONES]

    def consulta_hibrida():
        # 1. Redis decide EL QUÉ (top 5 estaciones críticas ahora) y EL PRESENTE (su ventana de 10 min)
        pipe = r.pipeline(transaction=False)
        pipe.zrevrange("ranking:flujo", 0, 4, withscores=True)
        for est in estaciones_ids:
            pipe.hget(f"ultimo:{est}", "ts")
        res_p1 = pipe.execute()
        top5 = res_p1[0]
        ancla = max(int(x) for x in res_p1[1:] if x is not None)
        corte = ancla - VENTANA_MS

        pipe_v = r.pipeline(transaction=False)
        for est, _ in top5:
            pipe_v.zrangebyscore(f"ventana:{est}", corte, ancla)
        ventanas_top5 = pipe_v.execute()

        # 2. HBase aporta EL CUÁNDO HISTÓRICO: misma franja [00:50, 01:00) en los 5 días anteriores
        resultados = []
        for (est, ultimo_val), miembros_vent in zip(top5, ventanas_top5):
            vals_ahora = [float(m.split(":", 1)[1]) for m in miembros_vent]
            prom_ahora = sum(vals_ahora) / len(vals_ahora)

            vals_hist = []
            for dia in dias_hist:
                ini = f"{est}#{dia}T00:50".encode()
                fin = f"{est}#{dia}T01:00".encode()
                for _, v in t_lect.scan(row_start=ini, row_stop=fin, batch_size=500):
                    vals_hist.append(float(v[b"m:valor"].decode()))

            prom_hist = sum(vals_hist) / len(vals_hist)
            var_hist = sum((x - prom_hist) ** 2 for x in vals_hist) / len(vals_hist)
            std_hist = math.sqrt(var_hist)
            diff_pct = ((prom_ahora - prom_hist) / prom_hist) * 100.0
            z_score = (prom_ahora - prom_hist) / std_hist if std_hist > 0 else 0.0

            resultados.append({
                "estacion": est,
                "ultimo_flujo": round(float(ultimo_val), 1),
                "prom_10m_redis": round(prom_ahora, 2),
                "prom_hist_hbase": round(prom_hist, 2),
                "std_hist_hbase": round(std_hist, 2),
                "lecturas_hist_5d": len(vals_hist),
                "desviacion_pct": round(diff_pct, 1),
                "z_score": round(z_score, 2),
                "estado": "SOBRE_PROMEDIO" if prom_ahora > prom_hist else "BAJO_PROMEDIO",
            })
        return resultados

    res_p6, t_p6, corridas_p6 = medir(consulta_hibrida)

    print("\n6.1 Resultado de la consulta híbrida (Top 5 flujo actual vs. historia 00:50–01:00):")
    print(pd.DataFrame(res_p6).to_string(index=False))
    print(f"\n6.2 Tiempo medido (mediana de 5 corridas): {t_p6*1000:.2f} ms (corridas: {[round(x*1000, 2) for x in corridas_p6]} ms)")
    print("   - Interpretación y justificación del reparto: Redis selecciona en sub-milisegundos las 5 estaciones críticas del instante (`ranking:flujo`) y su promedio móvil de 10 min (`ventana:*`) evitando escanear las 16 estaciones en disco, mientras que HBase resuelve únicamente los 5×5=25 micro-rangos históricos de esas 5 estaciones en la franja 00:50–01:00 (datos de días anteriores que ya fueron recortados de la RAM de Redis).")

    return {
        "t_hibrida_ms": round(t_p6 * 1000, 2),
        "resultados": res_p6,
    }


def main():
    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=60000)

    resumen = {
        "p1": p1_ahora(r, conn),
        "p2": p2_ventana_10min(r),
        "p3": p3_rango_hbase(conn),
        "p4": p4_dos_patrones(r, conn),
        "p5": p5_hotspotting_salting(conn),
        "p6": p6_propia(r, conn),
    }
    conn.close()

    ruta_json = RESULTADOS_DIR / "preguntas_resumen.json"
    ruta_json.write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n✅ Resumen de las 6 preguntas guardado en {ruta_json}")


if __name__ == "__main__":
    main()
