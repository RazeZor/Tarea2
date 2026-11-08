"""Orquestador de punta a punta de la Tarea 2 (Partes B, C y D)."""
import json
import subprocess
import sys
import time
from pathlib import Path
import redis

from config import (
    REDIS_HOST,
    REDIS_PORT,
    STREAM,
    GRUPO_INGESTA,
    ROOT_DIR,
    generar_datos,
)
from claves import clave_lecturas
import cargar_historico
from experimentos_fallas import ejecutar_d2, inspeccionar_versiones_d1
from verificar import verificar
import preguntas

RESULTADOS_DIR = ROOT_DIR / "resultados"


def ejecutar_camino_en_vivo_con_d1() -> dict:
    """Ejecuta la Parte B.2 (camino en vivo a 20×) integrando el experimento D.1:
    - `ingesta-1` muere con os._exit(1) en el lote 5 (después del put y antes del XACK).
    - Se inspecciona XPENDING (mostrando las 200 entradas pendientes a nombre de `ingesta-1`).
    - `ingesta-2` entra al rescate con XAUTOCLAIM y completa la hora en vivo sin perder ni duplicar filas.
    """
    print("\n" + "=" * 72)
    print("PARTE B.2 + D.1 — Camino en vivo (20×) con caída y recuperación de consumidor")
    print("=" * 72)

    py = sys.executable
    src_dir = ROOT_DIR / "src"

    # Lanzar productor (7.200 eventos a 25 ms), consumidor_tablero, e ingesta-1 (que morirá en el lote 5)
    proc_prod = subprocess.Popen([py, str(src_dir / "productor.py")])
    proc_tab = subprocess.Popen([py, str(src_dir / "consumidor_tablero.py")])
    proc_ing1 = subprocess.Popen([
        py, str(src_dir / "consumidor_ingesta.py"),
        "--consumidor", "ingesta-1",
        "--lote", "200",
        "--morir-en-lote", "5",
    ])

    # Esperar a que ingesta-1 muera en el lote 5 (ocurre a los ~1000 eventos = ~25 s)
    codigo_ing1 = proc_ing1.wait()
    print(f"\n--- D.1.1 Consumidor `ingesta-1` terminó con código de salida: {codigo_ing1} ---")

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    resumen_pend = r.xpending(STREAM, GRUPO_INGESTA)
    detalle_pend = r.xpending_range(STREAM, GRUPO_INGESTA, min="-", max="+", count=5)
    # Obtener el evento exacto que quedó pendiente en XPENDING para inspeccionar sus versiones en HBase
    _, ev_pendiente = r.xrange(STREAM, min=resumen_pend["min"], max=resumen_pend["min"], count=1)[0]
    print(f"  Estado de XPENDING en grupo `{GRUPO_INGESTA}` tras la muerte de `ingesta-1`:")
    print(f"    - Total pendientes: {resumen_pend['pending']}")
    print(f"    - Rango de IDs:     {resumen_pend['min']} .. {resumen_pend['max']}")
    print(f"    - Por consumidor:   {resumen_pend['consumers']}")
    print(f"    - Primeras 3 entradas en XPENDING: {detalle_pend[:3]}")

    # Lanzar ingesta-2 para recuperar con XAUTOCLAIM y terminar el resto de la hora en vivo
    print("\n--- D.1.2 Lanzando `ingesta-2` para recuperar pendientes (XAUTOCLAIM) y continuar ---")
    proc_ing2 = subprocess.Popen([
        py, str(src_dir / "consumidor_ingesta.py"),
        "--consumidor", "ingesta-2",
        "--lote", "200",
    ])

    proc_prod.wait()
    proc_tab.wait()
    proc_ing2.wait()

    # Inspeccionar la fila de `ev_pendiente` para mostrar las 2 versiones en HBase
    info_celda = inspeccionar_versiones_d1(clave_lecturas(ev_pendiente))

    return {
        "exit_code_ingesta_1": codigo_ing1,
        "xpending_tras_caida": {
            "pending": resumen_pend["pending"],
            "min": resumen_pend["min"],
            "max": resumen_pend["max"],
            "consumers": resumen_pend["consumers"],
        },
        "celda_reprocesada": info_celda,
    }


def main():
    t_global = time.perf_counter()
    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Experimento D.2 (prueba sin persistencia vs con AOF+volumen, dejando activa la config de Parte A)
    res_d2 = ejecutar_d2()
    (RESULTADOS_DIR / "experimento_d2.json").write_text(
        json.dumps(res_d2, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # 2. Parte B.1: Carga histórica (864.000 eventos directo a HBase) y preparación de Redis
    t_b1 = time.perf_counter()
    cargar_historico.main()
    dt_b1 = time.perf_counter() - t_b1

    # 3. Parte B.2 + D.1: Camino en vivo a 20× con caída y recuperación de consumidor
    res_d1 = ejecutar_camino_en_vivo_con_d1()
    (RESULTADOS_DIR / "experimento_d1.json").write_text(
        json.dumps(res_d1, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # 4. Parte B.3 + D.1.3: Verificación contra números de control
    ok = verificar()
    if not ok:
        sys.exit(1)

    # 5. Parte C: Las 6 preguntas con tiempos medidos
    preguntas.main()

    dt_total = time.perf_counter() - t_global
    print("\n" + "=" * 72)
    print(f"🎉 PIPELINE COMPLETO FINALIZADO EXITOSAMENTE EN {dt_total:.1f} s ({dt_total/60:.1f} min)")
    print(f"   (Carga histórica B.1: {dt_b1:.1f} s)")
    print("=" * 72)


if __name__ == "__main__":
    main()
