import os
import json
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


# -----------------------
# UTILS
# -----------------------

def log(msg, *args):
    """Imprime mensajes uniformes en CloudWatch"""
    print(f"[DEBUG] {msg}", *args)

def error_log(msg, *args):
    """Imprime mensajes de error en CloudWatch"""
    print(f"[ERROR] {msg}", *args)


def parse_iso_z(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def unir_fecha_hora(fecha_str, hora_str):
    fecha = parse_iso_z(fecha_str)
    hora = parse_iso_z(hora_str)
    if not fecha:
        return None
    if not hora:
        return fecha.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    try:
        nueva = fecha.replace(
            hour=hora.hour,
            minute=hora.minute,
            second=hora.second,
            microsecond=hora.microsecond
        )
        return nueva.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return fecha.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_convert_numbers(item):
    out = dict(item)
    for key in ("magnitud", "profundidad", "latitud", "longitud"):
        if key in out and out[key] not in (None, ""):
            try:
                val = out[key]
                if isinstance(val, str):
                    out[key] = Decimal(val)
                else:
                    out[key] = Decimal(str(val))
            except Exception:
                pass
    return out


def limpiar_tabla():
    log("Limpiando tabla DynamoDB...")
    resp = table.scan()
    items = resp.get("Items", [])

    total_deleted = 0

    while True:
        if not items:
            break

        with table.batch_writer() as batch:
            for it in items:
                if "id" in it:
                    batch.delete_item(Key={"id": it["id"]})
                    total_deleted += 1

        if "LastEvaluatedKey" in resp:
            resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            items = resp.get("Items", [])
        else:
            break

    log(f"Tabla limpiada. Items eliminados: {total_deleted}")


def insertar_items(items):
    log(f"Insertando {len(items)} items...")
    count = 0
    with table.batch_writer() as batch:
        for it in items:
            batch.put_item(Item=it)
            count += 1
    log(f"Insertados correctamente: {count}")
    return count


def obtener_sismos_por_anio(year):
    url = f"https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/{year}"
    log(f"Solicitando datos al IGP para el año {year}...")

    try:
        r = requests.get(url, timeout=15)
    except Exception as e:
        error_log(f"Error de request para año {year}: {e}")
        return {"ok": False, "error": str(e), "items": []}

    if r.status_code == 200:
        try:
            data = r.json()
            log(f"Datos obtenidos para {year}: {len(data)} items")
            return {"ok": True, "items": data}
        except Exception as e:
            error_log(f"JSON inválido para {year}: {e}")
            return {"ok": False, "error": str(e), "items": []}

    if r.status_code == 404:
        log(f"IGP no tiene datos para el año {year}.")
        return {"ok": True, "items": []}

    error_log(f"Error HTTP {r.status_code} para año {year}")
    return {"ok": False, "error": f"HTTP {r.status_code}", "items": []}


# -----------------------
# HANDLER PRINCIPAL
# -----------------------

def lambda_handler(event, context):
    log("Evento recibido:", json.dumps(event))

    try:
        # Leer body si viene desde HTTP API
        if isinstance(event, dict) and "body" in event and isinstance(event["body"], str):
            try:
                body = json.loads(event["body"])
            except:
                body = {}
        else:
            body = event or {}

        start = int(body.get("start_year", DEFAULT_START_YEAR))
        end = int(body.get("end_year", DEFAULT_END_YEAR))
        if end < start:
            start, end = end, start

        log(f"Procesando desde {start} hasta {end}...")

        resumen = {
            "years_processed": [],
            "total_inserted": 0,
            "errors": []
        }

        all_items_to_insert = []

        # Proceso año por año
        for year in range(start, end + 1):
            resp = obtener_sismos_por_anio(year)

            if not resp["ok"]:
                resumen["errors"].append({
                    "year": year,
                    "error": resp["error"]
                })
                continue

            items = resp["items"]
            processed = []

            for rec in items:
                r = dict(rec)
                r.setdefault("id", str(uuid.uuid4()))

                fecha_local = r.get("fecha_local")
                hora_local = r.get("hora_local")
                r["fecha_hora_local"] = unir_fecha_hora(fecha_local, hora_local)

                r["procesado_en"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

                r = safe_convert_numbers(r)
                processed.append(r)

            resumen["years_processed"].append({
                "year": year,
                "count": len(processed)
            })

            all_items_to_insert.extend(processed)

        # Limpiar tabla
        try:
            limpiar_tabla()
        except Exception as e:
            error_log("Error limpiando tabla:", e)
            return http_error(f"Error limpiando tabla: {e}")

        # Insertar items
        try:
            inserted = insertar_items(all_items_to_insert)
            resumen["total_inserted"] = inserted
        except Exception as e:
            error_log("Error insertando items:", e)
            return http_error(f"Error al insertar en DynamoDB: {e}")

        log("Proceso finalizado correctamente.")
        return http_ok(resumen)

    except Exception as e:
        error_log("Excepción no manejada:", e)
        return http_error(f"Error inesperado: {e}")


# -----------------------
# RESPUESTAS HTTP
# -----------------------

def http_ok(data):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(data, ensure_ascii=False)
    }

def http_error(msg):
    return {
        "statusCode": 500,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": msg}, ensure_ascii=False)
    }
