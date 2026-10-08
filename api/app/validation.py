"""Validacion de parametros de entrada. Funciones puras: lanzan ValidationError (-> HTTP 400)."""
import math

MAX_RADIUS_M = 50_000
MAX_LIMIT = 1000
DEFAULT_LIMIT = 50
MAX_POLYGON_VERTICES = 500
MIN_ID, MAX_ID = 1, 151


class ValidationError(ValueError):
    pass


def _number(args, name, required=True):
    raw = args.get(name)
    if raw is None or raw == "":
        if required:
            raise ValidationError(f"falta el parametro '{name}'")
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValidationError(f"'{name}' debe ser numerico")
    if math.isnan(value) or math.isinf(value):
        raise ValidationError(f"'{name}' no es un numero valido")
    return value


def parse_lat_lng(args):
    lat = _number(args, "lat")
    lng = _number(args, "lng")
    if not -90 <= lat <= 90:
        raise ValidationError("'lat' debe estar entre -90 y 90")
    if not -180 <= lng <= 180:
        raise ValidationError("'lng' debe estar entre -180 y 180")
    return lat, lng


def parse_radius(args, default=None):
    """Radio en metros (>0 y <= MAX_RADIUS_M)."""
    r = _number(args, "radius", required=default is None)
    if r is None:
        return float(default)
    if not 0 < r <= MAX_RADIUS_M:
        raise ValidationError(f"'radius' debe estar entre 0 (exclusivo) y {MAX_RADIUS_M} metros")
    return r


def parse_flag(args, name):
    """Parametro booleano opcional: true/false (insensible a mayusculas); ausente = False."""
    raw = args.get(name)
    if raw is None or raw == "":
        return False
    v = str(raw).lower()
    if v not in ("true", "false"):
        raise ValidationError(f"'{name}' debe ser true o false")
    return v == "true"


def parse_limit(args, default=DEFAULT_LIMIT, maximum=MAX_LIMIT):
    raw = args.get("limit")
    if raw is None or raw == "":
        return default
    try:
        n = int(raw)
    except (TypeError, ValueError):
        raise ValidationError("'limit' debe ser un entero")
    if not 1 <= n <= maximum:
        raise ValidationError(f"'limit' debe estar entre 1 y {maximum}")
    return n


def parse_pokemon_id(args, name="pokemonId"):
    raw = args.get(name)
    if raw is None or raw == "":
        return None
    try:
        n = int(raw)
    except (TypeError, ValueError):
        raise ValidationError(f"'{name}' debe ser un entero")
    if not MIN_ID <= n <= MAX_ID:
        raise ValidationError(f"'{name}' debe estar entre {MIN_ID} y {MAX_ID}")
    return n


def parse_type(args, valid, name="type"):
    """Tipo de pokemon (fire, water...). `valid` = tipos que existen en el catalogo."""
    raw = args.get(name)
    if raw is None or raw == "":
        return None
    t = raw.strip().lower()
    if t not in valid:
        raise ValidationError(f"'{name}' debe ser uno de: {', '.join(sorted(valid))}")
    return t


def _position(p):
    if not isinstance(p, (list, tuple)) or len(p) != 2:
        raise ValidationError("cada posicion del poligono debe ser [lng, lat]")
    lng, lat = p
    for v in (lng, lat):
        if isinstance(v, bool) or not isinstance(v, (int, float)) or math.isnan(v) or math.isinf(v):
            raise ValidationError("las coordenadas del poligono deben ser numeros")
    if not -180 <= lng <= 180 or not -90 <= lat <= 90:
        raise ValidationError("coordenada del poligono fuera de rango (lng -180..180, lat -90..90)")
    return [float(lng), float(lat)]


def parse_polygon(body):
    """Acepta una geometria GeoJSON Polygon, o un Feature que la contenga. Devuelve la geometria normalizada."""
    if not isinstance(body, dict):
        raise ValidationError("el cuerpo debe ser un objeto JSON (GeoJSON Polygon o Feature)")
    geom = body.get("geometry") if body.get("type") == "Feature" else body
    if not isinstance(geom, dict) or geom.get("type") != "Polygon":
        raise ValidationError("se espera un GeoJSON de tipo 'Polygon'")
    rings = geom.get("coordinates")
    if not isinstance(rings, list) or not rings:
        raise ValidationError("'coordinates' debe ser una lista de anillos")
    clean, total = [], 0
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ValidationError("cada anillo necesita al menos 4 posiciones (el primero se repite al final)")
        pts = [_position(p) for p in ring]
        if pts[0] != pts[-1]:
            raise ValidationError("cada anillo debe estar cerrado (primera y ultima posicion iguales)")
        total += len(pts)
        clean.append(pts)
    if total > MAX_POLYGON_VERTICES:
        raise ValidationError(f"el poligono excede {MAX_POLYGON_VERTICES} vertices")
    return {"type": "Polygon", "coordinates": clean}
