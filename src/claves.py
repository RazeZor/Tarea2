"""Diseño de row keys y columnas para HBase (Parte B y Pregunta 5)."""
import zlib
from datetime import datetime


def ts_a_ms(ts_iso: str) -> int:
    """Convierte un timestamp ISO 8601 a milisegundos desde 1970 (epoch ms)."""
    return int(round(datetime.fromisoformat(ts_iso).timestamp() * 1000))


def clave_lecturas(ev: dict) -> bytes:
    """Row key para `lecturas`: estacion_id#timestamp (medición en ISO-8601 con -03:00)."""
    return f"{ev['estacion_id']}#{ev['timestamp']}".encode()


def clave_lecturas_tipo(ev: dict) -> bytes:
    """Row key para `lecturas_tipo`: tipo_sensor#timestamp#estacion_id."""
    return f"{ev['tipo_sensor']}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def clave_ts_estacion(ev: dict) -> bytes:
    """Row key para P5 (ts#estacion): timestamp#estacion_id."""
    return f"{ev['timestamp']}#{ev['estacion_id']}".encode()


def salt_estacion(estacion_id: str) -> int:
    """Prefijo de salting determinista: hash(estacion) mod 4 usando CRC32."""
    return zlib.crc32(estacion_id.encode()) % 4


def clave_salt_ts_estacion(ev: dict) -> bytes:
    """Row key para P5 (salt#ts#estacion): (hash(estacion) mod 4)#timestamp#estacion_id."""
    s = salt_estacion(ev["estacion_id"])
    return f"{s}#{ev['timestamp']}#{ev['estacion_id']}".encode()


def columnas_lecturas(ev: dict) -> dict[bytes, bytes]:
    """Columnas guardadas en `lecturas` (y tablas de P5)."""
    return {
        b"m:valor": str(ev["valor"]).encode(),
        b"m:tipo": ev["tipo_sensor"].encode(),
    }


def columnas_lecturas_tipo(ev: dict) -> dict[bytes, bytes]:
    """Columnas guardadas en `lecturas_tipo`."""
    return {
        b"m:valor": str(ev["valor"]).encode(),
        b"m:estacion": ev["estacion_id"].encode(),
    }
