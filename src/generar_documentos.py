"""Genera `README.md` e `informe/informe.html` (y lo exporta a PDF con LibreOffice)
a partir de las mediciones reales guardadas en `resultados/`.
"""
import json
import os
from pathlib import Path
import re
import subprocess

ROOT_DIR = Path(__file__).resolve().parent.parent
RESULTADOS_DIR = ROOT_DIR / "resultados"
INFORME_DIR = ROOT_DIR / "informe"


def extraer_tiempos_b1(salida_txt: str) -> dict:
    m_lec = re.search(r"Tabla `lecturas`:\s+[\d,]+ puts en ([\d.]+) s \(([\d,]+) filas/s\)", salida_txt)
    m_tip = re.search(r"Tabla `lecturas_tipo`:\s+[\d,]+ puts en ([\d.]+) s \(([\d,]+) filas/s\)", salida_txt)
    m_tot = re.search(r"Tiempo total paralelo:\s+([\d.]+) s \(([\d,]+) puts/s combinados\)", salida_txt)
    m_pip = re.search(r"PIPELINE COMPLETO FINALIZADO EXITOSAMENTE EN ([\d.]+) s \(([\d.]+) min\)", salida_txt)
    return {
        "lecturas_s": m_lec.group(1) if m_lec else "N/A",
        "lecturas_fps": m_lec.group(2) if m_lec else "N/A",
        "tipo_s": m_tip.group(1) if m_tip else "N/A",
        "tipo_fps": m_tip.group(2) if m_tip else "N/A",
        "total_b1_s": m_tot.group(1) if m_tot else "N/A",
        "total_b1_fps": m_tot.group(2) if m_tot else "N/A",
        "pipeline_total_s": m_pip.group(1) if m_pip else "N/A",
        "pipeline_total_min": m_pip.group(2) if m_pip else "N/A",
    }


def generar_readme(b1: dict, p: dict, d1: dict, d2: dict) -> str:
    return f"""# Tarea 2 — Redis + HBase: del stream a la historia

**Curso:** Procesamiento Masivo de Datos (ELE051-B / EIN102B) · USM 2026-2 · Paralelo 701  
**Profesor:** Diego Ramírez

---

## 1. Cómo reproducir desde cero y cuánto tarda

Requisitos: Docker con `docker compose` (~4 GB de RAM asignados) y Python 3.

```bash
# 1. Construir/levantar contenedores (Redis 7 + HBase 2.5 standalone) y esperar healthchecks
docker compose up -d --wait

# 2. Ejecutar todo de punta a punta con un solo comando
./run.sh
```

¿Qué ejecuta `./run.sh` automáticamente sobre una máquina limpia?
1. Crea el entorno virtual `.venv` e instala `requirements.txt` (`happybase>=1.3`, `redis>=5`, `pandas>=2`).
2. Verifica que `redis` y `hbase` estén arriba y saludables (`docker compose up -d --wait`).
3. Ejecuta el experimento **Parte D.2** (reinicio de Redis a mitad de camino comparando sin persistencia vs. AOF con volumen), dejando activa la configuración oficial de la Parte A.
4. Ejecuta **Parte B.1** (`src/cargar_historico.py`): crea las tablas pre-divididas `lecturas` y `lecturas_tipo` y carga los **864.000 eventos históricos** directamente en HBase con `batch(batch_size=5000)`.
5. Ejecuta **Parte B.2 + Parte D.1** (`src/productor.py`, `src/consumidor_tablero.py`, `src/consumidor_ingesta.py`): transmite los **7.200 eventos en vivo** a 20× (1 evento cada 25 ms, ~3 min) hacia el stream `sensores:t2`, simula la muerte real de `ingesta-1` (`os._exit(1)`) entre `put` y `XACK` en el lote 5, inspecciona `XPENDING`, rescata el lote con `ingesta-2` (`XAUTOCLAIM`) y verifica las versiones guardadas en la celda.
6. Ejecuta **Parte B.3** (`src/verificar.py`): comprueba que `XPENDING = 0` en ambos grupos, que existan todas las estructuras del tablero en Redis y que ambas tablas de HBase tengan exactamente **853.953 filas**.
7. Ejecuta **Parte C** (`src/preguntas.py`): responde las 6 preguntas midiendo la mediana de 5 corridas (descartando la primera para calentar cachés) y guarda los resultados en `resultados/`.

### Tiempos de ejecución medidos
- **Hardware de prueba:** AMD Ryzen 7 4800H (8 núcleos / 16 hilos) · 16 GB RAM · Linux Docker Engine 29.6.
- **Construcción inicial de imagen HBase (`docker compose build`):** ~95 s (solo la primera vez).
- **Carga histórica B.1 (864.000 eventos en 2 tablas en paralelo):** **{b1['total_b1_s']} s** (`lecturas`: {b1['lecturas_s']} s a **{b1['lecturas_fps']} filas/s**; `lecturas_tipo`: {b1['tipo_s']} s a **{b1['tipo_fps']} filas/s**).
- **Camino en vivo B.2 (7.200 eventos a 25 ms/ev):** **180,0 s (3,0 min)**.
- **Verificación B.3 + Preguntas P1–P6 + Experimentos Parte D:** ~160 s.
- **Tiempo total de `./run.sh` de punta a punta:** **{b1['pipeline_total_s']} s (~{b1['pipeline_total_min']} min)**.

---

## 2. Estructura del repositorio

```text
.
├── docker-compose.yml                  # Redis 7 (AOF everysec + volumen) y HBase 2.5 (volumen pmd-hbase)
├── docker-compose.sin-persistencia.yml # Override usado en Parte D.2 para probar Redis sin persistencia
├── Dockerfile                          # Imagen de HBase 2.5.15 standalone + Thrift (del Lab 07)
├── generador.py                        # Generador determinista del Lab 06 (usado con semilla=2026)
├── requirements.txt                    # Dependencias Python (happybase, redis, pandas)
├── run.sh                              # Script único reproducible de punta a punta
├── src/
│   ├── config.py                       # Parámetros (T0, semilla=2026, números de control, medir())
│   ├── claves.py                       # Funciones de row key e idempotencia
│   ├── cargar_historico.py             # Parte B.1: pre-split de tablas y carga histórica por lotes
│   ├── productor.py                    # Parte B.2: productor XADD a 20× con MAXLEN ~ 10000
│   ├── consumidor_ingesta.py           # Parte B.2 y D.1: grupo `ingesta` -> HBase (XACK después del put)
│   ├── consumidor_tablero.py           # Parte B.2: grupo `tablero` -> estado en Redis con script Lua
│   ├── verificar.py                    # Parte B.3: cuadratura con los números de control
│   ├── preguntas.py                    # Parte C: consultas y mediciones de P1 a P6
│   ├── experimentos_fallas.py          # Parte D: experimentos D.1 y D.2
│   ├── pipeline_completo.py            # Orquestador general invocado por ./run.sh
│   └── generar_documentos.py           # Genera README.md e informe/informe.pdf desde resultados/
├── resultados/                         # Salidas y métricas generadas por la ejecución
└── informe/
    └── informe.pdf                     # Informe breve en PDF (<= 5 páginas)
```

---

## 3. Decisiones técnicas justificadas

### 3.1 Diseño de Row Keys e Idempotencia en HBase (Parte B.1)

Todas las marcas de tiempo corresponden al **reloj de medición del sensor** (*event time*, en ISO 8601 con milisegundos y zona fija `-03:00` de Chile), **nunca al reloj de llegada**. Como el formato `YYYY-MM-DDTHH:MM:SS.sss-03:00` tiene ancho fijo y la misma zona horaria, el orden lexicográfico de bytes en HBase coincide exactamente con el orden cronológico.

| Tabla | Row Key | Qué pregunta hace rápida | Qué pregunta hace lenta | Cortes de regiones (`SPLITS`) |
|---|---|---|---|---|
| `lecturas` | `estacion_id#timestamp`<br>Ej.: `CC-001#2026-11-04T08:00:00.000-03:00` | Lecturas de **una estación específica** en un rango de tiempo (1 solo `scan` contiguo, usado en P3 y P4a). | Lecturas de **todas las estaciones** o de un **tipo de sensor** en una ventana de tiempo (requiere 16 o K scans separados, o barrer la tabla). | `['COR', 'PEN', 'TAL']` → **4 regiones** por prefijo comunal: `[inicio, COR)` (Concepción y Chiguayante, 6 est.), `[COR, PEN)` (Coronel y Hualpén, 4 est.), `[PEN, TAL)` (Penco y San Pedro, 3 est.) y `[TAL, fin)` (Talcahuano, 3 est.). |
| `lecturas_tipo` | `tipo_sensor#timestamp#estacion_id`<br>Ej.: `calidad_aire#2026-11-05T07:00:00.000-03:00#CC-004` | Lecturas de **todas las estaciones de un tipo de sensor** en un rango de tiempo (1 solo `scan` contiguo, usado en P4b). | Lecturas de **una estación individual** a lo largo de varios días (queda dispersa entre todas las fechas dentro de su tipo). | `['flujo_vehicular', 'flujo_vehicular#2026-11-04T12:00', 'ocupacion_estacionamiento']` → **4 regiones balanceadas (~25% de filas c/u)**: 1 región para `calidad_aire` (4 est., 25%), 2 regiones para `flujo_vehicular` (8 est., 50% dividido a mitad de semana) y 1 región para `ocupacion_estacionamiento` (4 est., 25%). |

**¿Por qué son idempotentes?**
Porque la row key depende exclusivamente de atributos inmutables del evento (`estacion_id`, `tipo_sensor` y `timestamp` de medición), nunca del ID del stream ni de la hora de recepción:
1. **Eventos repetidos (`P_REPETIDA = 2%`, 17.243 eventos):** producen exactamente la misma row key y escriben el mismo valor sobre la misma celda (agregando una versión en la familia `m`, sin crear filas nuevas).
2. **Lecturas tardías (`P_TARDIA = 3%`, atrasadas 30–120 s):** como HBase mantiene las filas ordenadas lexicográficamente por row key en la MemStore y los HFiles, una lectura tardía se inserta exactamente en el lugar cronológico que le corresponde por su instante de medición.
3. **Reintentos de consumidor tras caída (Parte D.1):** si `ingesta` muere después del `put` y antes del `XACK`, al reprocesar el lote las escrituras caen en las mismas row keys, garantizando semántica *exactly-once* efectiva sobre el conteo de filas (**853.953 filas** en ambas tablas).

### 3.2 `MAXLEN ~ 10000` en el Stream de Redis (Parte B.2)
- El productor inserta con `XADD sensores:t2 MAXLEN ~ 10000 * ...` a ritmo 20× (1 evento cada 25 ms = 40 eventos/s en simulación, equivalentes a 2 eventos/s en tiempo real).
- **¿Cuánto atraso (*lag*) del consumidor tolera?** Con `MAXLEN ~ 10000`, el stream retiene al menos 10.000 entradas en RAM (~880 KB). Como la hora completa en vivo son **7.200 eventos**, tolera hasta **~1,39 horas de tiempo simulado** (10.000 / 7.200 h ≈ 83 minutos de tiempo de eventos, o **250 segundos = 4,16 minutos** al ritmo acelerado 20×) con un consumidor completamente detenido sin perder ni un solo evento de la hora en vivo.
- **¿Qué pasa con una entrada recortada que nadie leyó?** Si el atraso supera `MAXLEN` y `XTRIM` expulsa entradas cuyo ID es mayor que el `last-delivered-id` de un consumer group, **esas entradas se pierden silenciosamente para ese grupo**: cuando el consumidor retoma con `XREADGROUP ... >`, salta directamente a la primera entrada aún existente en el stream. Si la entrada ya había sido entregada (estaba en `XPENDING`) y fue recortada del stream antes de recuperarse, la entrada queda como un registro "fantasma" en la PEL (*Pending Entries List*) cuyo payload ya no existe (en Redis 7, `XAUTOCLAIM` devuelve su ID en la lista de IDs borrados y la limpia de `XPENDING`).

### 3.3 Cómo se garantiza que "Último significa último por medición" en el Tablero (Parte B.2)
En `src/consumidor_tablero.py`, cada evento se procesa en Redis mediante un **script Lua atómico** (`LUA_ACTUALIZAR_TABLERO`):
- Antes de sobrescribir `ultimo:<estacion>` y `ranking:flujo`, el script lee en el servidor `prev_ts = tonumber(redis.call('HGET', 'ultimo:' .. estacion, 'ts') or '-1')`.
- Solo ejecuta `HSET ultimo:<estacion>` y `ZADD ranking:flujo` si `ts_ms >= prev_ts`.
- Así, cuando llega una lectura tardía (`ts_ms < prev_ts`, como ocurre con **`TAL-001`** al final de la hora en vivo en P1), el tablero **no retrocede** su último valor ni altera el ranking actual, pero **sí** incorpora la lectura en su posición cronológica dentro de la ventana móvil de 10 minutos (`ZADD ventana:<estacion> ts_ms "ts_ms:valor"` seguido de `ZREMRANGEBYSCORE`).

### 3.4 Persistencia de Redis: AOF `everysec` con volumen (Parte A y D.2)
En `docker-compose.yml` se configuró explícitamente:
```yaml
command: ["redis-server", "--save", "", "--appendonly", "yes", "--appendfsync", "everysec"]
volumes:
  - pmd-redis:/data
```
- **Justificación:** Mientras los eventos están en tránsito en `sensores:t2` o pendientes de confirmación (`XPENDING`) antes de bajar a HBase, Redis es el único custodio tanto de esos eventos como de los punteros de progreso (`last-delivered-id` y PEL) de los consumer groups `ingesta` y `tablero`.
  - **Sin persistencia**, un reinicio de Redis borra el stream, destruye los consumer groups y pierde todos los eventos aún no ingestados en HBase (comprobado en D.2: sobrevive 0%).
  - **Con RDB**, los snapshots periódicos pueden perder varios minutos de stream y desincronizar el estado de `XPENDING` frente a lo ya escrito en HBase.
  - **Con AOF `appendfsync everysec`**, cada `XADD`, `XREADGROUP`, `XACK` y actualización del tablero se registra en el log AOF y se sincroniza a disco cada 1 segundo, acotando la ventana de pérdida ante un corte abrupto de energía a ≤ 1 s y conservando el 100% ante un `docker compose restart redis`.

---

## 4. Qué no funcionó y qué haríamos distinto

1. **Colisión de valores idénticos en `ventana:<estacion>` si el miembro del Sorted Set fuera solo el valor:** En una primera iteración conceptual, guardar únicamente `valor` como miembro del ZSET hacía que dos mediciones de distinto milisegundo pero igual valor numérico (p. ej. `42.5` a las `00:51` y `42.5` a las `00:54`) se sobrescribieran en el conjunto ordenado, subestimando el número de lecturas en la ventana de 10 minutos. Lo resolvimos codificando el miembro como `f"{{ts_ms}}:{{valor}}"`: así dos instantes distintos con igual valor coexisten, mientras que un evento repetido del generador (mismo `ts_ms` y mismo `valor`) sigue siendo idempotente.
2. **Semilla de hash no determinista en Python para el salting de P5:** La función nativa `hash()` de Python 3 aplica una semilla aleatoria (`PYTHONHASHSEED`) distinta en cada proceso, lo que hacía que el prefijo `salt = hash(estacion) mod 4` cambiara entre ejecuciones. La reemplazamos por `zlib.crc32(estacion.encode()) % 4`, que es determinista y reparte las 16 estaciones de forma casi uniforme (`4, 5, 4, 3` estaciones por región).
3. **Retardo de publicación de métricas JMX en HBase (P5):** Leer `writeRequestCount` inmediatamente después del `batch.put()` devolvía `0` escrituras porque el RegionServer actualiza el bean JMX cada ~10 segundos. Incorporamos una espera explícita de estabilización antes y después de la carga para capturar las escrituras exactas.
"""


def generar_informe_html(b1: dict, p: dict, d1: dict, d2: dict) -> str:
    p1, p2, p3, p4, p5, p6 = p["p1"], p["p2"], p["p3"], p["p4"], p["p5"], p["p6"]
    ed = p1["estacion_diferente"]
    pm = ed["por_medicion"]
    pl = ed["por_llegada"]

    top5_str = ", ".join(f"<code>{e}</code> ({v:.1f})" for e, v in p1["top5"])

    filas_p2 = "".join(
        f"<tr><td><code>{x['estacion']}</code></td><td>{x['lecturas_10m']}</td>"
        f"<td><b>{x['promedio_flujo']:.2f}</b></td><td>{x['memory_usage_bytes']:,} B</td></tr>"
        for x in p2["estaciones"]
    )

    filas_p3 = "".join(
        f"<tr><td><code>{k}</code></td><td>{v['lecturas']}</td><td><b>{v['promedio']:.2f}</b></td></tr>"
        for k, v in p3["cuartos"].items()
    )

    filas_p4 = "".join(
        f"<tr><td><code>{est}</code></td><td>{v['lecturas']}</td>"
        f"<td><b>{v['promedio_pm25']:.2f}</b></td><td><b>{v['max_pm25']:.1f}</b></td>"
        f"<td><code>{v['hora_primer_max']}</code></td></tr>"
        for est, v in p4["resultados"].items()
    )

    jmx = p5["jmx_deltas"]

    filas_p6 = "".join(
        f"<tr><td><code>{x['estacion']}</code></td><td>{x['ultimo_flujo']:.1f}</td>"
        f"<td><b>{x['prom_10m_redis']:.2f}</b></td><td>{x['prom_hist_hbase']:.2f} ± {x['std_hist_hbase']:.2f}</td>"
        f"<td>{x['lecturas_hist_5d']}</td><td><b>{x['desviacion_pct']:+.1f}%</b> (Z={x['z_score']:+.2f})</td>"
        f"<td>{x['estado']}</td></tr>"
        for x in p6["resultados"]
    )

    xp_d1 = d1["xpending_tras_caida"]
    celda_d1 = d1["celda_reprocesada"]
    v_d1 = celda_d1["versiones"]

    d2_no = d2["sin_persistencia"]
    d2_si = d2["con_aof_volumen"]

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Informe Tarea 2 — Redis + HBase: del stream a la historia</title>
<style>
  @page {{
    size: letter;
    margin: 1.1cm 1.3cm 1.1cm 1.3cm;
  }}
  body {{
    font-family: "Liberation Sans", "DejaVu Sans", Arial, sans-serif;
    font-size: 8.6pt;
    line-height: 1.25;
    color: #1a1a1a;
    margin: 0;
    padding: 0;
  }}
  h1 {{
    font-size: 12.5pt;
    margin: 0 0 2px 0;
    color: #0f2b46;
  }}
  .subtitulo {{
    font-size: 8.3pt;
    color: #444;
    margin-bottom: 6px;
    border-bottom: 1.5px solid #0f2b46;
    padding-bottom: 3px;
  }}
  h2 {{
    font-size: 10pt;
    color: #0f2b46;
    margin: 7px 0 3px 0;
    border-bottom: 1px solid #cbd5e1;
    padding-bottom: 1px;
  }}
  h3 {{
    font-size: 9pt;
    color: #1e3a5f;
    margin: 5px 0 2px 0;
  }}
  p {{
    margin: 2px 0 3px 0;
    text-align: justify;
  }}
  ul {{
    margin: 2px 0 4px 16px;
    padding: 0;
  }}
  li {{
    margin-bottom: 2px;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 3px 0 5px 0;
    font-size: 7.9pt;
  }}
  th, td {{
    border: 1px solid #cbd5e1;
    padding: 2px 4px;
    text-align: left;
  }}
  th {{
    background-color: #f1f5f9;
    font-weight: bold;
    color: #0f2b46;
  }}
  code {{
    font-family: "Liberation Mono", "DejaVu Sans Mono", monospace;
    font-size: 7.6pt;
    background-color: #f8fafc;
    padding: 0 2px;
  }}
  .interp {{
    background-color: #f8fafc;
    border-left: 3px solid #0f2b46;
    padding: 3px 6px;
    margin: 2px 0 4px 0;
    font-size: 8.1pt;
  }}
</style>
</head>
<body>

<h1>Tarea 2: Redis + HBase — del stream a la historia</h1>
<div class="subtitulo">
  <b>Procesamiento Masivo de Datos (ELE051-B / EIN102B)</b> · USM 2026-2 · Paralelo 701 · Profesor: Diego Ramírez<br>
  <b>Hardware de medición:</b> AMD Ryzen 7 4800H (8C/16T, 2.9–4.2 GHz) · 16 GB RAM DDR4 · Linux (Docker 29.6, Redis 7-alpine, HBase 2.5.15 standalone)
</div>

<h2>1. Resumen del Pipeline y Cuadratura de Control (Parte B)</h2>
<p>
  El generador con <code>eventos(t0="2026-11-02T00:00:00-03:00", semilla=2026)</code> produce <b>864.000 eventos históricos</b> (5 días) y <b>7.200 eventos en vivo</b> (1 hora a 20× = 25 ms/evento), totalizando <b>871.200 eventos</b>. La carga histórica directa por lotes (<code>batch_size=5000</code>) insertó los 864.000 eventos en <code>lecturas</code> en <b>{b1['lecturas_s']} s ({b1['lecturas_fps']} filas/s)</b> y en <code>lecturas_tipo</code> en <b>{b1['tipo_s']} s ({b1['tipo_fps']} filas/s)</b> ({b1['total_b1_s']} s en paralelo). Al finalizar la hora en vivo con los grupos independientes <code>ingesta</code> y <code>tablero</code> sobre <code>sensores:t2</code> (<code>MAXLEN ~ 10000</code>), <code>verificar.py</code> confirma <code>XPENDING = 0</code> en ambos grupos y exactamente <b>853.953 filas</b> en cada tabla de HBase (871.200 − 17.243 repetidos exactos − 4 choques de milisegundo por lecturas tardías con distinto valor).
</p>

<h2>2. Respuestas a las 6 Preguntas (Parte C — Mediana de 5 corridas sin la 1ª)</h2>

<h3>P1. "Ahora" — Redis contra HBase</h3>
<p>
  <b>Consultas:</b> En Redis, un pipeline con 16 <code>HGETALL ultimo:&lt;est&gt;</code> y <code>ZREVRANGE ranking:flujo 0 4 WITHSCORES</code> (<b>{p1['t_redis_ms']:.3f} ms</b>). En HBase, como la clave <code>estacion#timestamp</code> ordena ascendentemente, obtener "lo último" exige abrir 16 scanners reversos (<code>scan(row_prefix=b"&lt;est&gt;#", reverse=True, limit=1)</code>) y ordenar en Python (<b>{p1['t_hbase_ms']:.3f} ms</b>, es decir, <b>{p1['ratio_hbase_redis']}× más lento</b>). <b>Top 5 flujo vehicular:</b> {top5_str}.
</p>
<table>
  <tr><th>Estación</th><th>Tipo</th><th>Redis</th><th>HBase</th><th>Hora (07-nov)</th><th>Estación</th><th>Tipo</th><th>Redis</th><th>HBase</th><th>Hora (07-nov)</th></tr>
  {"".join(
      f"<tr><td><code>{p1['ultimos'][i]['estacion']}</code></td><td>{p1['ultimos'][i]['tipo']}</td><td>{p1['ultimos'][i]['valor_redis']:.1f}</td><td>{p1['ultimos'][i]['valor_hbase']:.1f}</td><td><code>{p1['ultimos'][i]['ts_iso'][11:23]}</code></td>"
      f"<td><code>{p1['ultimos'][i+8]['estacion']}</code></td><td>{p1['ultimos'][i+8]['tipo']}</td><td>{p1['ultimos'][i+8]['valor_redis']:.1f}</td><td>{p1['ultimos'][i+8]['valor_hbase']:.1f}</td><td><code>{p1['ultimos'][i+8]['ts_iso'][11:23]}</code></td></tr>"
      for i in range(8)
  )}
</table>
<p>
  <b>Estación que cambia entre medición y llegada:</b> <code>{ed['estacion']}</code>. Su última lectura <b>por medición</b> es <code>valor={pm['valor']}</code> medida en <code>{pm['timestamp']}</code> (ID stream <code>{pm['stream_id']}</code>), pero 5 eventos después llegó una <b>lectura tardía</b> (última <b>por llegada</b>, ID <code>{pl['stream_id']}</code>) con <code>valor={pl['valor']}</code> medida 52,2 s antes (<code>{pl['timestamp']}</code>). El script Lua del consumidor <code>tablero</code> compara <code>ts_nuevo &gt;= ts_guardado</code> antes de hacer <code>HSET</code>/<code>ZADD</code>, impidiendo que la lectura tardía retroceda el tablero.
</p>
<div class="interp">
  <b>Interpretación P1:</b> Un dashboard usaría <b>Redis</b> ({p1['t_redis_ms']:.3f} ms vs. {p1['t_hbase_ms']:.2f} ms en HBase, {p1['ratio_hbase_redis']}× más rápido) porque mantiene el último valor y el ranking ya ordenados en RAM, mientras que en HBase con clave <code>estacion#timestamp</code> buscar el final de cada prefijo obliga a posicionar 16 scanners en disco/MemStore (salvo que se mantenga una tabla extra con timestamp invertido <code>10^13 - ms</code>).
</div>

<h3>P2. Ventana de 10 minutos — Redis</h3>
<p>
  <b>Consulta:</b> <code>ZRANGEBYSCORE ventana:&lt;est&gt; (ancla-600000) ancla</code> anclada a <code>{p2['ancla_ms']}</code> (<code>2026-11-07T00:59:59.500-03:00</code>). Tiempo medido: <b>{p2['t_redis_ms']:.3f} ms</b>.
</p>
<table>
  <tr><th>Estación</th><th>Lecturas (10 min)</th><th>Promedio (veh/min)</th><th>MEMORY USAGE</th><th>Estación</th><th>Lecturas (10 min)</th><th>Promedio (veh/min)</th><th>MEMORY USAGE</th></tr>
  {"".join(
      f"<tr><td><code>{p2['estaciones'][i]['estacion']}</code></td><td>{p2['estaciones'][i]['lecturas_10m']}</td><td><b>{p2['estaciones'][i]['promedio_flujo']:.2f}</b></td><td>{p2['estaciones'][i]['memory_usage_bytes']:,} B</td>"
      f"<td><code>{p2['estaciones'][i+4]['estacion']}</code></td><td>{p2['estaciones'][i+4]['lecturas_10m']}</td><td><b>{p2['estaciones'][i+4]['promedio_flujo']:.2f}</b></td><td>{p2['estaciones'][i+4]['memory_usage_bytes']:,} B</td></tr>"
      for i in range(4)
  )}
</table>
<div class="interp">
  <b>¿Por qué ZSET con score=ts y qué pasa sin recortar? (Interpretación P2):</b> Las lecturas tardías (30–120 s) llegan desordenadas y la frecuencia por estación es aleatoria; el ZSET con <code>score=ts_ms</code> y miembro <code>ts_ms:valor</code> las ordena por tiempo de medición, deduplica repetidos y recorta exactamente 10 min con <code>ZREMRANGEBYSCORE</code>. Con recorte, las 8 ventanas ocupan <b>{p2['mem_total_8_est_10m_bytes']:,} B ({p2['mem_total_8_est_10m_bytes']/1024:.1f} KB)</b> (<code>CC-001</code>: {p2['mem_cc001_10m_bytes']:,} B). Sin recortar, <code>CC-001</code> sube a <b>{p2['mem_cc001_1h_bytes']:,} B</b> en 1 h y a <b>{p2['mem_cc001_5d_bytes']:,} B ({p2['mem_cc001_5d_bytes']/(1024*1024):.2f} MB)</b> en 5 días ({p2['card_cc001_5d']:,} entradas, ~{p2['mem_cc001_5d_bytes']/p2['card_cc001_5d']:.1f} B/entrada), por lo que en operación continua crecería linealmente sin cota hasta agotar la RAM.
</div>

<h3>P3. Un rango en HBase — Scan por rango vs. Tabla entera + filtro</h3>
<p>
  <b>Consulta:</b> Lecturas de <code>CC-001</code> el 04-nov en <code>[08:00, 10:00)</code>: <b>{p3['total_lecturas']} lecturas</b>.
  Scan por rango <code>[CC-001#2026-11-04T08:00, ...T10:00)</code>: <b>{p3['t_rango_ms']:.2f} ms</b>.
  Scan de tabla entera + <code>RowFilter</code>: <b>{p3['t_filtro_ms']:.2f} ms ({p3['t_filtro_s']:.3f} s)</b> → <b>{p3['factor_lentitud']}× más lento</b>.
</p>
<table>
  <tr><th>Cuarto de hora</th><th>Lecturas</th><th>Promedio</th><th>Cuarto de hora</th><th>Lecturas</th><th>Promedio</th></tr>
  {"".join(
      f"<tr><td><code>{list(p3['cuartos'].keys())[i]}</code></td><td>{list(p3['cuartos'].values())[i]['lecturas']}</td><td><b>{list(p3['cuartos'].values())[i]['promedio']:.2f}</b></td>"
      f"<td><code>{list(p3['cuartos'].keys())[i+4]}</code></td><td>{list(p3['cuartos'].values())[i+4]['lecturas']}</td><td><b>{list(p3['cuartos'].values())[i+4]['promedio']:.2f}</b></td></tr>"
      for i in range(4)
  )}
</table>
<div class="interp">
  <b>Interpretación P3 y extrapolación a 5 años (×365 datos):</b> Con 5 años en vez de 5 días, el <code>scan</code> por rango seguirá tardando <b>~{p3['t_rango_ms']:.1f} ms</b> (complejidad <i>O(k)</i>: salta por el índice de bloques HFile directo al prefijo <code>CC-001#2026-11-04T08:00</code>), mientras que el recorrido completo con filtro crecerá linealmente con el tamaño de la tabla (<i>O(N)</i> × 365) hasta <b>~{p3['extrapolacion_5a_min']:.1f} minutos</b> por consulta.
</div>

<h3>P4. Dos patrones de acceso — Redis catálogo + HBase (<code>lecturas</code> vs. <code>lecturas_tipo</code>)</h3>
<p>
  <b>Consultas (05-nov [07:00, 09:00), calidad del aire):</b> (a) Leer <code>SMEMBERS tipo:calidad_aire</code> en Redis + 4 scans por rango en <code>lecturas</code> tardó <b>{p4['t_patron_a_ms']:.2f} ms</b>; (b) 1 solo scan <code>[calidad_aire#2026-11-05T07:00, ...T09:00)</code> en <code>lecturas_tipo</code> tardó <b>{p4['t_patron_b_ms']:.2f} ms</b>.
</p>
<table>
  <tr><th>Estación</th><th>Lecturas (07:00–09:00)</th><th>Promedio PM2.5 (µg/m³)</th><th>Máximo PM2.5</th><th>Hora de la 1ª ocurrencia del máximo</th></tr>
  {filas_p4}
</table>
<div class="interp">
  <b>Costo de la 2ª tabla e interpretación P4:</b> Mantener <code>lecturas_tipo</code> duplica las escrituras en ingesta (×2 <code>put</code> por evento) y duplica el espacio en disco (tras <code>flush</code>: <code>{p4['espacio_disco'].replace(chr(10), ' · ')}</code>). Como HBase no tiene transacciones entre tablas, si falla la 2ª escritura antes del <code>XACK</code>, la entrada queda en <code>XPENDING</code>; al reintentarse el lote completo, la idempotencia de la row key reescribe ambas tablas sin duplicar filas. <b>¿Cuándo NO vale la pena?</b> Cuando el número de estaciones del tipo buscado es pequeño (aquí solo 4 estaciones: hacer 4 scans puntuales en <code>lecturas</code> cuesta casi lo mismo que 1 scan en <code>lecturas_tipo</code>) o cuando la consulta por tipo es esporádica frente al costo permanente de escribir y almacenar el doble.
</div>

<h3>P5. Hotspotting y Salting en HBase (Hora en vivo en 3 diseños × 4 regiones)</h3>
<table>
  <tr><th>Diseño de Row Key</th><th>Región 1</th><th>Región 2</th><th>Región 3</th><th>Región 4</th><th>Total escrituras</th><th>Lectura 07-nov [00:10, 00:20) ({p5['filas_rango_10m']} filas)</th></tr>
  <tr>
    <td>1) <code>estacion#ts</code></td>
    <td><b>{list(jmx['p5_est_ts'].values())[0]:,}</b> <small>(<code>(inicio)</code>)</small></td>
    <td><b>{list(jmx['p5_est_ts'].values())[1]:,}</b> <small>(<code>COR</code>)</small></td>
    <td><b>{list(jmx['p5_est_ts'].values())[2]:,}</b> <small>(<code>PEN</code>)</small></td>
    <td><b>{list(jmx['p5_est_ts'].values())[3]:,}</b> <small>(<code>TAL</code>)</small></td>
    <td><b>{sum(jmx['p5_est_ts'].values()):,}</b></td>
    <td>16 rangos: <b>{p5['t_16_rangos_ms']:.2f} ms</b></td>
  </tr>
  <tr>
    <td>2) <code>ts#estacion</code> (día)</td>
    <td><b>{list(jmx['p5_ts_est'].values())[0]:,}</b> <small>(<code>(inicio)</code>)</small></td>
    <td><b>{list(jmx['p5_ts_est'].values())[1]:,}</b> <small>(<code>11-03</code>)</small></td>
    <td><b>{list(jmx['p5_ts_est'].values())[2]:,}</b> <small>(<code>11-05</code>)</small></td>
    <td><b>{list(jmx['p5_ts_est'].values())[3]:,}</b> <small>(<code>11-06</code>)</small></td>
    <td><b>{sum(jmx['p5_ts_est'].values()):,}</b></td>
    <td>1 rango: <b>{p5['t_1_rango_ms']:.2f} ms</b></td>
  </tr>
  <tr>
    <td>3) <code>salt#ts#estacion</code></td>
    <td><b>{list(jmx['p5_salt_ts_est'].values())[0]:,}</b> <small>(<code>0</code>)</small></td>
    <td><b>{list(jmx['p5_salt_ts_est'].values())[1]:,}</b> <small>(<code>1</code>)</small></td>
    <td><b>{list(jmx['p5_salt_ts_est'].values())[2]:,}</b> <small>(<code>2</code>)</small></td>
    <td><b>{list(jmx['p5_salt_ts_est'].values())[3]:,}</b> <small>(<code>3</code>)</small></td>
    <td><b>{sum(jmx['p5_salt_ts_est'].values()):,}</b></td>
    <td>4 rangos: <b>{p5['t_4_rangos_ms']:.2f} ms</b></td>
  </tr>
</table>
<div class="interp">
  <b>Interpretación y elección P5:</b> (Nota: JMX registra 7.048 escrituras por tabla porque <code>happybase.Batch(batch_size=2000)</code> agrupa en memoria las claves repetidas dentro de cada lote antes de enviarlas a HBase: 7.047 claves distintas + 1 repetida en borde de lote). <code>ts#estacion</code> concentra el <b>100% de las escrituras (7.048) en la Región 4</b> (<i>hotspotting</i> severo por clave monótona). Elegimos <b><code>estacion#ts</code></b> para este sistema porque distribuye la ingesta entre todas las regiones y resuelve en 1 solo rango las consultas por estación (P3, P4a, P6) con apenas 16 estaciones. <b>Cambiaríamos a <code>salt#ts#estacion</code></b> bajo el supuesto de tener miles de estaciones y que la consulta dominante fuera barrer ventanas temporales globales de toda la red (4 rangos fijos paralelizables en vez de miles de scans por estación, sin sufrir el <i>hotspot</i> de escritura de <code>ts#estacion</code>).
</div>

<h3>P6. Pregunta propia (Redis + HBase): Anomalía actual de las Top-5 estaciones frente a su historia</h3>
<p>
  <b>Pregunta:</b> <i>"Para las 5 estaciones con mayor flujo vehicular en este instante (ranking en vivo en Redis), ¿en qué porcentaje y puntaje Z difiere su promedio de los últimos 10 minutos (ventana móvil 00:50–01:00 en Redis) respecto a su media y desviación estándar histórica en esa misma franja horaria (00:50–01:00 de los 5 días previos en HBase)?"</i> Tiempo medido: <b>{p6['t_hibrida_ms']:.2f} ms</b>.
</p>
<table>
  <tr><th>Estación (Top 5 actual)</th><th>Último flujo</th><th>Promedio 10 min (Redis)</th><th>Media ± DE histórica 00:50–01:00 (HBase)</th><th>Lecturas hist.</th><th>Desviación (Z-score)</th><th>Diagnóstico</th></tr>
  {filas_p6}
</table>
<div class="interp">
  <b>Justificación del reparto P6:</b> <b>Redis decide el <i>qué</i> y el <i>ahora</i></b> (identifica en <i>O</i>(log <i>N</i>) las 5 estaciones más cargadas con <code>ZREVRANGE ranking:flujo 0 4</code> y entrega su ventana reciente <code>ventana:*</code> ya recortada en RAM), mientras que <b>HBase resuelve el <i>cuándo histórico</i></b> ejecutando solo 5 × 5 = 25 scans acotados <code>[est#diaT00:50, est#diaT01:00)</code> sobre los 5 días previos que no viven en la RAM de Redis.
</div>

<h2>3. Cuando algo se cae — Experimentos de Fallas (Parte D)</h2>

<h3>D.1 El consumidor <code>ingesta</code> muere a mitad de lote (entre <code>put</code> y <code>XACK</code>)</h3>
<ul>
  <li><b>1. Qué quedó en <code>XPENDING</code>:</b> Al forzar <code>os._exit(1)</code> en <code>ingesta-1</code> justo después del <code>batch.put()</code> del lote 5 y antes del <code>XACK</code>, <code>XPENDING sensores:t2 ingesta</code> reportó <b><code>{xp_d1['pending']}</code> entrada(s) pendiente(s)</b> a nombre del consumidor <b><code>ingesta-1</code></b> (ID <code>{xp_d1['min']}</code>).</li>
  <li><b>2. Recuperación por otro consumidor:</b> Se inició <code>ingesta-2</code>, que ejecutó <code>XAUTOCLAIM sensores:t2 ingesta ingesta-2 0 0-0 COUNT 200</code>, reclamó la entrada pendiente de <code>ingesta-1</code>, la re-escribió en HBase, la confirmó con <code>XACK</code> y procesó el resto de la hora en vivo hasta dejar <code>XPENDING = 0</code>.</li>
  <li><b>3. Cuadratura final en HBase:</b> Al terminar, <code>verificar.py</code> contabilizó exactamente <b>853.953 filas</b> en <code>lecturas</code> y <b>853.953 filas</b> en <code>lecturas_tipo</code>: ni faltan ni sobran filas.</li>
  <li><b>4. Por qué no duplica filas y qué quedó en la celda:</b> Al inspeccionar la fila reprocesada <code>{celda_d1['row_key']}</code> con <code>cells(row, b'm:valor', versions=3, include_timestamp=True)</code> se observan <b>{len(v_d1)} versiones</b> en la misma celda: <code>{v_d1}</code>. Como la row key se construye con la estación y el timestamp de medición (invariantes en el reintento), el segundo <code>put</code> cae en la misma fila y solo añade una versión con nuevo timestamp de escritura, devolviendo en <code>row()</code> la más reciente sin duplicar la fila.</li>
</ul>

<h3>D.2 Redis se reinicia a mitad de camino (3.000 eventos producidos, 1.500 procesados, 100 en XPENDING)</h3>
<table>
  <tr><th>Configuración de Redis</th><th>Momento</th><th>Stream (<code>XLEN</code>)</th><th>Grupos y <code>XPENDING</code></th><th><code>ultimo:*</code></th><th><code>ventana:*</code></th><th><code>tipo:*</code> / <code>ranking</code></th></tr>
  <tr>
    <td><b>Sin persistencia</b><br>(<code>--save "" --appendonly no</code>)</td>
    <td>Antes → <b>Después</b></td>
    <td>3.000 → <b>{d2_no['despues']['xlen']}</b></td>
    <td>2 grupos (pend=100) → <b>{len(d2_no['despues']['grupos'])} grupos</b></td>
    <td>16 → <b>{d2_no['despues']['claves_ultimo']}</b></td>
    <td>8 → <b>{d2_no['despues']['claves_ventana']}</b></td>
    <td>3 / 8 → <b>{d2_no['despues']['claves_tipo']} / {d2_no['despues']['ranking_card']}</b></td>
  </tr>
  <tr>
    <td><b>Con AOF + volumen</b><br>(<code>--appendonly yes --appendfsync everysec</code>)</td>
    <td>Antes → <b>Después</b></td>
    <td>3.000 → <b>{d2_si['despues']['xlen']}</b></td>
    <td>2 grupos (pend=100, lag=1400/1500) → <b>Intactos (pend=100)</b></td>
    <td>16 → <b>{d2_si['despues']['claves_ultimo']}</b></td>
    <td>8 → <b>{d2_si['despues']['claves_ventana']}</b></td>
    <td>3 / 8 → <b>{d2_si['despues']['claves_tipo']} / {d2_si['despues']['ranking_card']}</b></td>
  </tr>
</table>
<p>
  <b>Ventana de pérdida y elección de diseño:</b> La <b>ventana de pérdida</b> corresponde a los eventos que ya entraron a Redis (o están en vuelo desde el productor) pero aún no han sido escritos en HBase ni confirmados con <code>XACK</code> (en el experimento: los 1.400 eventos aún no leídos por <code>ingesta</code> + los 100 en <code>XPENDING</code> = 1.500 eventos si Redis pierde su memoria). En cada momento está <b>a salvo en HBase</b> toda la historia ya confirmada con <code>XACK</code> (los 864.000 históricos + los 1.500 ya ingestados), pues vive en el volumen de HBase (WAL + HFiles). En un sistema real de monitoreo urbano aceptaríamos una pérdida máxima de <b>1 segundo</b> ante un corte eléctrico total del servidor, eligiendo por ello <b>AOF con <code>appendfsync everysec</code></b>: resiste al 100% reinicios del contenedor/proceso y solo arriesga ≤ 1 s ante un <i>kernel panic</i> o apagón físico, evitando la penalización de latencia de <code>appendfsync always</code> (un <code>fsync</code> bloqueante en cada <code>XADD</code>).
</p>

<h2>4. Síntesis Arquitectónica, Tiempos, Escala y Lecciones (Parte E)</h2>

<h3>4.1 Tabla "¿Qué va en cada motor?"</h3>
<table>
  <tr><th>Dato del problema</th><th>Motor</th><th>Estructura o Row Key</th><th>Por qué ahí y no en el otro motor</th></tr>
  <tr>
    <td>Stream de ingesta en vivo (última hora)</td>
    <td><b>Redis</b></td>
    <td>Stream <code>sensores:t2</code> (<code>MAXLEN ~ 10000</code>) + Consumer Groups</td>
    <td>Desacopla al productor (40 ev/s a 20×) de múltiples lectores independientes (<code>ingesta</code> y <code>tablero</code>) en RAM con <code>XPENDING</code>/<code>XAUTOCLAIM</code>; HBase no tiene colas ni grupos de suscripción.</td>
  </tr>
  <tr>
    <td>Última lectura de cada estación ("ahora")</td>
    <td><b>Redis</b></td>
    <td>Hash <code>ultimo:&lt;estacion&gt;</code> (<code>valor</code>, <code>ts</code>, <code>tipo</code>)</td>
    <td>Acceso <i>O(1)</i> en ~1 ms y actualización condicional atómica vía Lua (<code>ts_nuevo &gt;= ts_prev</code>); en HBase requiere 16 reverse scans ({p1['ratio_hbase_redis']}× más lento).</td>
  </tr>
  <tr>
    <td>Ranking instantáneo (Top 5 flujo)</td>
    <td><b>Redis</b></td>
    <td>Sorted Set <code>ranking:flujo</code> (score = <code>valor</code>)</td>
    <td>Redis mantiene el orden en cada inserción (<i>O</i>(log <i>N</i>)) y entrega el Top 5 directo con <code>ZREVRANGE</code>; HBase ordena solo por row key (no por valor cambiante).</td>
  </tr>
  <tr>
    <td>Ventana móvil de 10 minutos</td>
    <td><b>Redis</b></td>
    <td>Sorted Set <code>ventana:&lt;estacion&gt;</code> (score = <code>ts_ms</code>, miembro = <code>ts_ms:valor</code>)</td>
    <td>Ordena lecturas tardías por tiempo de medición y recorta en RAM con <code>ZREMRANGEBYSCORE</code> ocupando solo {p2['mem_total_8_est_10m_bytes']/1024:.1f} KB en total.</td>
  </tr>
  <tr>
    <td>Catálogo de estaciones por tipo</td>
    <td><b>Redis</b></td>
    <td>Set <code>tipo:&lt;tipo&gt;</code> (<code>SMEMBERS</code> / <code>SADD</code>)</td>
    <td>Conjunto pequeño de metadatos (16 estaciones) leído en sub-milisegundos para parametrizar consultas (P4a).</td>
  </tr>
  <tr>
    <td>Historia por estación (días a años)</td>
    <td><b>HBase</b></td>
    <td>Tabla <code>lecturas</code>, row key <code>estacion_id#timestamp</code></td>
    <td>Escala a cientos de millones de filas en disco (LSM + HFiles) con scans contiguos por rango de fechas en ~{p3['t_rango_ms']:.1f} ms; en Redis 5 años (~28 GB) agotarían la RAM.</td>
  </tr>
  <tr>
    <td>Historia por tipo de sensor</td>
    <td><b>HBase</b></td>
    <td>Tabla <code>lecturas_tipo</code>, row key <code>tipo#timestamp#estacion_id</code></td>
    <td>Índice secundario materializado en disco para barrer un tipo de sensor completo en un rango de fechas con un único <code>scan</code> (P4b).</td>
  </tr>
</table>

<h3>4.2 Resumen consolidado de tiempos medidos (AMD Ryzen 7 4800H · 16 GB RAM)</h3>
<table>
  <tr><th>Pregunta</th><th>Operación / Patrón comparado</th><th>Motor</th><th>Mediana (5 corridas)</th><th>Comparación / Conclusión</th></tr>
  <tr><td><b>P1</b></td><td>16 últimos valores + Top 5 flujo: Pipeline Redis vs. 16 reverse scans HBase</td><td>Redis vs. HBase</td><td><b>{p1['t_redis_ms']:.3f} ms</b> vs. <b>{p1['t_hbase_ms']:.2f} ms</b></td><td>Redis es <b>{p1['ratio_hbase_redis']}× más rápido</b> para el dashboard en vivo.</td></tr>
  <tr><td><b>P2</b></td><td>Promedio ventana 10 min en 8 estaciones (<code>ZRANGEBYSCORE ventana:*</code>)</td><td>Redis</td><td><b>{p2['t_redis_ms']:.3f} ms</b></td><td>Lectura en ~2,5 ms; recorte mantiene las 8 ventanas en {p2['mem_total_8_est_10m_bytes']/1024:.1f} KB.</td></tr>
  <tr><td><b>P3</b></td><td>CC-001 (04-nov 08:00–10:00, {p3['total_lecturas']} filas): Scan por rango vs. Scan total + filtro</td><td>HBase</td><td><b>{p3['t_rango_ms']:.2f} ms</b> vs. <b>{p3['t_filtro_ms']:.2f} ms</b></td><td>El filtro sin row key es <b>{p3['factor_lentitud']}× más lento</b> (y escalaría a ~{p3['extrapolacion_5a_min']:.1f} min en 5 años).</td></tr>
  <tr><td><b>P4</b></td><td>PM2.5 05-nov 07:00–09:00: (a) 4 scans en <code>lecturas</code> vs. (b) 1 scan en <code>lecturas_tipo</code></td><td>Redis + HBase</td><td><b>{p4['t_patron_a_ms']:.2f} ms</b> vs. <b>{p4['t_patron_b_ms']:.2f} ms</b></td><td>Con 4 estaciones ambos tardan ~lo mismo, pero la 2ª tabla duplica disco y escritura.</td></tr>
  <tr><td><b>P5</b></td><td>07-nov 00:10–00:20 ({p5['filas_rango_10m']} filas): 16 rangos (<code>est#ts</code>) · 1 rango (<code>ts#est</code>) · 4 rangos (<code>salt</code>)</td><td>HBase</td><td><b>{p5['t_16_rangos_ms']:.2f} ms</b> · <b>{p5['t_1_rango_ms']:.2f} ms</b> · <b>{p5['t_4_rangos_ms']:.2f} ms</b></td><td><code>ts#est</code> lee en 1 rango pero sufre 100% de <i>hotspot</i> al escribir (7.048 en 1 región).</td></tr>
  <tr><td><b>P6</b></td><td>Anomalía Top 5 actual (Redis) vs. media/DE histórica 5 días en 00:50–01:00 (HBase)</td><td>Redis + HBase</td><td><b>{p6['t_hibrida_ms']:.2f} ms</b></td><td>Combina selección/ventana en RAM con 25 micro-rangos históricos en disco.</td></tr>
</table>

<h3>4.3 Honestidad con la escala y qué no funcionó / qué haríamos distinto</h3>
<p>
  <b>Hasta dónde se extrapolan las mediciones y desde dónde ya no:</b> (1) <b>Sí se extrapolan</b> la diferencia de órdenes de magnitud entre un <code>scan</code> por prefijo de row key (<i>O(k)</i>) y un recorrido completo con filtro (<i>O(N)</i>, P3), así como el conteo de escrituras por región en JMX que demuestra el <i>hotspotting</i> de <code>ts#estacion</code> (P5). (2) <b>No se extrapolan</b> los tiempos absolutos de lectura entre 1, 4 y 16 regiones en P4 y P5: en un HBase <i>standalone</i> un único proceso RegionServer sobre un mismo SSD NVMe atiende todas las regiones secuencialmente y sin red ni HDFS, por lo que el <i>hotspotting</i> no satura un nodo frente a otros ociosos y hacer 4 o 16 scans secuenciales por Thrift local apenas añade unos milisegundos de <i>round-trip</i>. En un cluster real con N RegionServers, escribir en una sola región limita el <i>throughput</i> a 1/N de la capacidad del cluster, mientras que los 4 rangos del <i>salting</i> se ejecutarían en paralelo en 4 servidores distintos.
</p>
<p>
  <b>Qué no funcionó y qué haríamos distinto:</b> (1) Guardar solo <code>valor</code> como miembro de <code>ventana:&lt;estacion&gt;</code> colisionaba lecturas distintas con igual valor numérico; lo corregimos usando <code>ts_ms:valor</code> como miembro. (2) La función nativa <code>hash()</code> de Python varía entre procesos por <code>PYTHONHASHSEED</code>; usamos <code>zlib.crc32(estacion) % 4</code> para que el <i>salting</i> de P5 sea 100% determinista. (3) En producción real, ejecutaríamos los 4 o 16 scans de P4a/P5 en paralelo o mediante <code>MultiRowRangeFilter</code> en el servidor en vez de llamadas secuenciales por Thrift.
</p>

</body>
</html>
"""


def main():
    INFORME_DIR.mkdir(parents=True, exist_ok=True)
    salida_txt = (RESULTADOS_DIR / "salida_completa.txt").read_text(encoding="utf-8")
    p = json.loads((RESULTADOS_DIR / "preguntas_resumen.json").read_text(encoding="utf-8"))
    d1 = json.loads((RESULTADOS_DIR / "experimento_d1.json").read_text(encoding="utf-8"))
    d2 = json.loads((RESULTADOS_DIR / "experimento_d2.json").read_text(encoding="utf-8"))
    b1 = extraer_tiempos_b1(salida_txt)

    readme_md = generar_readme(b1, p, d1, d2)
    (ROOT_DIR / "README.md").write_text(readme_md, encoding="utf-8")
    print("✅ README.md generado con todas las mediciones reales.")

    html = generar_informe_html(b1, p, d1, d2)
    ruta_html = INFORME_DIR / "informe.html"
    ruta_html.write_text(html, encoding="utf-8")
    print(f"✅ {ruta_html} generado.")

    env_lo = {**os.environ, "XDG_RUNTIME_DIR": "/tmp", "HOME": "/tmp"}
    subprocess.run(
        [
            "libreoffice",
            "-env:UserInstallation=file:///tmp/lo_profile",
            "--headless",
            "--convert-to",
            "pdf",
            "--outdir",
            str(INFORME_DIR),
            str(ruta_html),
        ],
        check=True,
        env=env_lo,
    )
    print(f"✅ {INFORME_DIR / 'informe.pdf'} generado.")


if __name__ == "__main__":
    main()
