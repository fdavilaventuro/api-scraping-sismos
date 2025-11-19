import json
import boto3
import traceback

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Sismos")

def safe_log(message):
    print(f"[DEBUG] {message}", flush=True)

def lambda_handler(event, context):
    safe_log("Lambda START")

    report = {
        "years_processed": [],
        "total_inserted": 0,
        "errors": []
    }

    try:
        # =====================================================
        # 1. SCRAPING (tu lógica original aquí)
        # =====================================================
        # Ejemplo ficticio:
        data = [
            {
                "id": "2025-001",
                "fecha": "2025-01-01 10:00:00",
                "magnitud": 5.4,
                "profundidad": 33,
                "lugar": "Lima"
            }
        ]

        year = 2025

        inserted_count = 0

        # =====================================================
        # 2. INSERCIÓN EN DYNAMODB
        # =====================================================
        safe_log(f"Insertando {len(data)} items en DynamoDB...")

        for row in data:
            try:
                response = table.put_item(Item=row)
                inserted_count += 1
                safe_log(f"Insert OK: {row['id']} | Response: {response['ResponseMetadata']['HTTPStatusCode']}")

            except Exception as e:
                err_msg = f"Error insertando item {row}: {e}"
                safe_log(err_msg)
                report["errors"].append(err_msg)
                safe_log(traceback.format_exc())

        # Agregar reporte por año
        report["years_processed"].append({
            "year": year,
            "count": inserted_count
        })

        report["total_inserted"] += inserted_count

        safe_log("Inserción completada")

    except Exception as e:
        # Error general
        err = f"ERROR GENERAL: {e}"
        safe_log(err)
        safe_log(traceback.format_exc())
        report["errors"].append(err)

        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(report, ensure_ascii=False)
        }

    safe_log("Lambda END")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(report, ensure_ascii=False)
    }
