"""Reglas de limpieza por registro (funcion pura, sin Dask ni I/O), documentadas en
docs/PLAN_DE_TRABAJO.md sec. 7 y fundamentadas en el perfilado (sec. 2.8).

clean_record(doc) -> (registro_limpio | None, motivo_de_descarte | None)

Motivos de descarte, en orden de evaluacion:
  coordenadas_invalidas  location ausente/mal formada, no numerica, fuera de rango o (0,0)
  sin_pokemonId          falta pokemonId o no esta en 1..151
  fecha_invalida         appearedOn.$date ausente o no interpretable como ISO 8601

La deduplicacion es global y se hace despues, con Dask (no es una regla por registro).
"""
import math
from datetime import datetime, timedelta, timezone

MIN_ID, MAX_ID = 1, 151

COLUMNS = [
    "id", "pokemonId", "source", "lng", "lat",
    "appeared_utc", "appeared_local", "local_hour", "local_dow",
]


def _coords(doc):
    loc = doc.get("location")
    if not isinstance(loc, dict) or loc.get("type") != "Point":
        return None
    c = loc.get("coordinates")
    if not isinstance(c, list) or len(c) != 2:
        return None
    lng, lat = c
    for v in (lng, lat):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or math.isnan(v):
            return None
    if abs(lat) > 90 or abs(lng) > 180 or (lat == 0 and lng == 0):
        return None
    return float(lng), float(lat)


def _utc(doc):
    ao = doc.get("appearedOn")
    s = ao.get("$date") if isinstance(ao, dict) else None
    if not isinstance(s, str):
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def local_time(utc, lng):
    """Hora local aproximada (solar): UTC + lng/15 horas = lng*4 minutos.

    `localTime` del dataset no es hora local real (ignora el horario de verano y en Europa
    ni acierta la zona; ver sec. 2.8), por eso se deriva de UTC y la longitud.
    """
    return utc + timedelta(minutes=round(lng * 4))


def clean_record(doc):
    coords = _coords(doc)
    if coords is None:
        return None, "coordenadas_invalidas"
    pid = doc.get("pokemonId")
    if isinstance(pid, bool) or not isinstance(pid, int) or not MIN_ID <= pid <= MAX_ID:
        return None, "sin_pokemonId"
    utc = _utc(doc)
    if utc is None:
        return None, "fecha_invalida"

    lng, lat = coords
    local = local_time(utc, lng)
    oid = doc.get("_id")
    return {
        "id": oid.get("$oid") if isinstance(oid, dict) else None,
        "pokemonId": pid,
        "source": str(doc.get("source")),
        "lng": lng,
        "lat": lat,
        "appeared_utc": utc,
        "appeared_local": local,
        "local_hour": local.hour,
        "local_dow": local.weekday(),  # 0 = lunes
    }, None
