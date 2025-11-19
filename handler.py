# handler.py
import os
import requests
import boto3
import uuid
from datetime import datetime, timezone
from decimal import Decimal

# Config
TABLE_NAME = os.environ.get("TABLE_NAME", "TablaWebScrapping")
AWS_REGION = "us-east-1"
DEFAULT_START_YEAR = int(os.environ.get("START_YEAR", "2025"))
DEFAULT_END_YEAR = int(os.environ.get("END_YEAR", str(DEFAULT_START_YEAR)))

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)

def parse_iso_z(s):
    """Parse ISO Z string like '2025-01-01T00:00:00.000Z' into aware UTC datetime."""
    if not s:
        return None
    try:
        # Python 3.11: fromisoformat doesn't accept Z, so replace with +00:00
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except Exception:
            try:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except Exception:
                return None

def unir_fecha_hora(fecha_str, hora_str):
    """
    Une fecha (fecha_str: '2025-01-01T00:00:00.000Z') y hora
    (hora_str: '1970-01-01T12:08:29.000Z' placeholder) en un único datetime UTC.
    Devuelve ISO string (Z).
    """
    fecha = parse_iso_z(fecha_str)
    hora = parse_iso_z(hora_str)
    if not fecha:
        return None
    if not hora:
        # si no hay hora, devolver fecha tal cual
        return fecha.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    # sustituir hora/min/segundos de fecha por los de hora
    try:
        nueva = fecha.replace(hour=hora.hour, minute=hora.minute, second=hora.second,
                              microsecond=hora.microsecond)
        return nueva.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return fecha.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_convert_numbers(item):
    """
    Convertir campos que deberían ser numéricos a Decimal (DynamoDB friendly).
    Ajusta según las claves que esperes: magnitud, profundidad, latitud, longitud.
    """
    out = dict(item)  # shallow copy
    # campos típicos
    for key in ("magnitud", "profundidad", "latitud", "longitud"):
        if key in out and out[key] is not None and out[key] != "":
            val = out[key]
            # si es string que representa número, convertir
            try:
                # tratar enteros y floats
                if isinstance(val, str):
                    if "." in val:
                        num = Decimal(val)
                    else:
                        # puede ser '4' -> Decimal('4')
                        num = Decimal(val)
                elif isinstance(val, (int, float, Decimal)):
                    num = Decimal(str(val))
                else:
                    continue
                out[key] = num
            except Exception:
                # dejar tal cual si falla
                out[key] = val
    return out

def limpiar_tabla():
    """Eliminar todos los items de la tabla (scan + batch delete)."""
    resp = table.scan()
    items = resp.get("Items", [])
    while True:
        if not items:
            break
        with table.batch_writer() as batch:
            for it in items:
                # asumimos que existe 'id' como clave primaria
                if "id" in it:
                    batch.delete_item(Key={"id": it["id"]})
                else:
                    # Si la tabla tiene otra PK, ajusta aquí
                    pass
        # paginación
        if 'LastEvaluatedKey' in resp:
            resp = table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
            items = resp.get("Items", [])
        else:
            break

def insertar_items(items):
    """Insertar items (lista de dict) usando batch_writer"""
    count = 0
    with table.batch_writer() as batch:
        for it in items:
            batch.put_item(Item=it)
            count += 1
    return count

def obtener_sismos_por_anio(year):
    url = f"https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/{year}"
    try:
        r = requests.get(url, timeout=15)
    except requests.RequestException as e:
        return {"ok": False, "error": str(e), "items": []}

    if r.status_code == 200:
        try:
            data = r.json()
            return {"ok": True, "items": data}
        except Exception as e:
            return {"ok": False, "error": f"JSON parse error: {e}", "items": []}
    elif r.status_code == 404:
        return {"ok": True, "items": []}  # no hay datos para ese año
    else:
        return {"ok": False, "error": f"HTTP {r.status_code}", "items": []}

def lambda_handler(event, context):
    """
    event puede contener:
      - start_year: int
      - end_year: int
    También toma START_YEAR/END_YEAR de env vars si no vienen en event.
    """
    start = int(event.get("start_year", DEFAULT_START_YEAR))
    end = int(event.get("end_year", DEFAULT_END_YEAR))
    if end < start:
        start, end = end, start

    resumen = {"years_processed": [], "total_inserted": 0, "errors": []}
    all_items_to_insert = []

    for y in range(start, end + 1):
        resp = obtener_sismos_por_anio(y)
        if not resp["ok"]:
            resumen["errors"].append({"year": y, "error": resp.get("error")})
            continue
        items = resp["items"]
        if not items:
            # no hay datos para ese año, continuar
            resumen["years_processed"].append({"year": y, "count": 0})
            continue

        processed = []
        for rec in items:
            # copiar para no mutar el original
            r = dict(rec)

            # crear id único si no viene
            r.setdefault("id", str(uuid.uuid4()))

            # unir fecha + hora
            fecha_local = r.get("fecha_local")
            hora_local = r.get("hora_local")
            r["fecha_hora_local"] = unir_fecha_hora(fecha_local, hora_local)

            # agregar timestamps de procesado
            r["procesado_en"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            # convertir campos numéricos a Decimal para DynamoDB
            r = safe_convert_numbers(r)

            processed.append(r)

        all_items_to_insert.extend(processed)
        resumen["years_processed"].append({"year": y, "count": len(processed)})

    # Limpiar tabla antes de insertar
    try:
        limpiar_tabla()
    except Exception as e:
        return {"statusCode": 500, "body": f"Error limpiando tabla: {e}"}

    # Insertar
    try:
        inserted = insertar_items(all_items_to_insert)
        resumen["total_inserted"] = inserted
    except Exception as e:
        return {"statusCode": 500, "body": f"Error al insertar en DynamoDB: {e}"}

    return {"statusCode": 200, "body": resumen}

