# Tarea 2 — Redis + HBase: del stream a la historia

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
- **Carga histórica B.1 (864.000 eventos en 2 tablas en paralelo):** **32.14 s** (`lecturas`: 32.14 s a **26,881 filas/s**; `lecturas_tipo`: 32.09 s a **26,924 filas/s**).
- **Camino en vivo B.2 (7.200 eventos a 25 ms/ev):** **180,0 s (3,0 min)**.
- **Verificación B.3 + Preguntas P1–P6 + Experimentos Parte D:** ~160 s.
- **Tiempo total de `./run.sh` de punta a punta:** **396.4 s (~6.6 min)**.

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

1. **Colisión de valores idénticos en `ventana:<estacion>` si el miembro del Sorted Set fuera solo el valor:** En una primera iteración conceptual, guardar únicamente `valor` como miembro del ZSET hacía que dos mediciones de distinto milisegundo pero igual valor numérico (p. ej. `42.5` a las `00:51` y `42.5` a las `00:54`) se sobrescribieran en el conjunto ordenado, subestimando el número de lecturas en la ventana de 10 minutos. Lo resolvimos codificando el miembro como `f"{ts_ms}:{valor}"`: así dos instantes distintos con igual valor coexisten, mientras que un evento repetido del generador (mismo `ts_ms` y mismo `valor`) sigue siendo idempotente.
2. **Semilla de hash no determinista en Python para el salting de P5:** La función nativa `hash()` de Python 3 aplica una semilla aleatoria (`PYTHONHASHSEED`) distinta en cada proceso, lo que hacía que el prefijo `salt = hash(estacion) mod 4` cambiara entre ejecuciones. La reemplazamos por `zlib.crc32(estacion.encode()) % 4`, que es determinista y reparte las 16 estaciones de forma casi uniforme (`4, 5, 4, 3` estaciones por región).
3. **Retardo de publicación de métricas JMX en HBase (P5):** Leer `writeRequestCount` inmediatamente después del `batch.put()` devolvía `0` escrituras porque el RegionServer actualiza el bean JMX cada ~10 segundos. Incorporamos una espera explícita de estabilización antes y después de la carga para capturar las escrituras exactas.
