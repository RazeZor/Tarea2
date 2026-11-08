"""Configuración centralizada y utilidades comunes para la Tarea 2."""
import itertools
import statistics
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from generador import ESTACIONES, RANGOS, eventos  # noqa: E402

TZ_CL = timezone(timedelta(hours=-3))
T0 = datetime.fromisoformat("2026-11-02T00:00:00-03:00")
SEMILLA = 2026

N_HIST = 864_000      # 5 días × 172.800 eventos/día
N_VIVO = 7_200        # 1 hora × 7.200 eventos/hora
N_TOTAL = N_HIST + N_VIVO

# Números de control oficiales del enunciado
CTRL_HIST_EVENTOS = 864_000
CTRL_HIST_REPETIDOS = 17_090
CTRL_HIST_CLAVES = 846_906

CTRL_VIVO_EVENTOS = 7_200
CTRL_VIVO_REPETIDOS = 153
CTRL_VIVO_CLAVES = 7_047

CTRL_TOTAL_EVENTOS = 871_200
CTRL_TOTAL_REPETIDOS = 17_243
CTRL_TOTAL_CLAVES = 853_953

# Parámetros de Redis y HBase
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
HBASE_HOST = "127.0.0.1"
HBASE_PORT = 9090

STREAM = "sensores:t2"
MAXLEN = 10_000       # > 7.200 eventos de la hora en vivo: tolera > 1 h de atraso
RITMO_VIVO = 0.025    # 20× el ritmo real (0.5 s / 20 = 25 ms por evento)

GRUPO_INGESTA = "ingesta"
GRUPO_TABLERO = "tablero"

TABLA_LECTURAS = "lecturas"
TABLA_LECTURAS_TIPO = "lecturas_tipo"

VENTANA_MS = 10 * 60 * 1000  # 10 minutos en milisegundos (600.000 ms)


def generar_datos():
    """Genera de forma determinista el bloque histórico (864.000) y en vivo (7.200)."""
    gen = eventos(T0, semilla=SEMILLA)
    historico = list(itertools.islice(gen, N_HIST))
    en_vivo = list(itertools.islice(gen, N_VIVO))
    return historico, en_vivo


def hbase_shell(comandos: str) -> str:
    """Ejecuta comandos en el `hbase shell` del contenedor y limpia el ruido de salida."""
    p = subprocess.run(
        ["docker", "compose", "exec", "-T", "hbase", "hbase", "shell", "-n"],
        input=comandos,
        capture_output=True,
        text=True,
        cwd=ROOT_DIR,
    )
    ruido = ("SLF4J", "hbase:", "Took ", "HBase Shell", 'Use "help"', "For Reference", "Version ")
    lineas = [
        line for line in (p.stdout + p.stderr).splitlines()
        if line.strip() and not line.startswith(ruido)
    ]
    return "\n".join(lineas)


def en_contenedor_hbase(cmd: str) -> str:
    """Ejecuta un comando sh dentro del contenedor de HBase."""
    p = subprocess.run(
        ["docker", "compose", "exec", "-T", "hbase", "sh", "-c", cmd],
        capture_output=True,
        text=True,
        cwd=ROOT_DIR,
    )
    return p.stdout.strip()


def contar_filas_hbase(conn, nombre_tabla: str) -> int:
    """Cuenta filas en HBase usando FirstKeyOnlyFilter() AND KeyOnlyFilter() (Lab 07, 2.2)."""
    filtro = "FirstKeyOnlyFilter() AND KeyOnlyFilter()"
    return sum(1 for _ in conn.table(nombre_tabla).scan(filter=filtro, batch_size=5000))


def medir(fn, corridas: int = 5):
    """Ejecuta 1 corrida de calentamiento (descartada) y N corridas medidas.
    Devuelve (resultado, mediana_segundos, lista_tiempos_segundos).
    """
    resultado = fn()  # 1ª corrida: calienta cachés (se descarta)
    tiempos = []
    for _ in range(corridas):
        t0 = time.perf_counter()
        resultado = fn()
        tiempos.append(time.perf_counter() - t0)
    return resultado, statistics.median(tiempos), tiempos
