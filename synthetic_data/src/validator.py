# -*- coding: utf-8 -*-
# synthetic_data/src/validator.py

import os
import re
import csv
import json
from datetime import datetime
from utils.config import load_config
from utils.logging import setup_logger

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'shared', 'config', 'config.yaml')
config = load_config(CONFIG_PATH)

log_path = os.path.join('synthetic_data', 'logs')
logger = setup_logger(PROJECT_ROOT, log_path, 'validator')

seed = config['inyector']['seed']
INPUT_JSON = os.path.join(BASE_DIR, 'data', config['inyector']['data_name'])
OUTPUT_REPORT_DIR = os.path.join(BASE_DIR, 'output')


EMAIL_REGEX = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

# EMAIL_REGEX = r'^[A-Za-z0-9._%+-ñÑáéíóúÁÉÍÓÚüÜ]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'


def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def generar_nombre_archivo(prefix, seed, ext):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    return "%s_%s_seed%s.%s" % (prefix, timestamp, seed, ext)


def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)


def save_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_csv(path, data):
    if not data:
        with open(path, 'w') as f:
            f.write("")
        return

    fieldnames = [
        'row_index',
        'customer_id',
        'error_type',
        'rule',
        'field',
        'message'
    ]

    with open(path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def validar_email(email):
    if not isinstance(email, str):
        return False
    return re.match(EMAIL_REGEX, email) is not None


def validar_telefono(telefono):
    if not isinstance(telefono, str):
        return False
    return telefono.isdigit() and len(telefono) == 10 and telefono.startswith("09")


def validar_fecha_nacimiento(fecha_str):
    if not isinstance(fecha_str, str):
        return False
    try:
        datetime.strptime(fecha_str, "%d-%m-%Y")
        return True
    except:
        return False


def validar_fecha_creacion(fecha_str):
    if not isinstance(fecha_str, str):
        return False
    try:
        datetime.strptime(fecha_str, "%d/%m/%Y %H:%M:%S")
        return True
    except:
        return False


def validar_cedula_basica(cedula):
    if not isinstance(cedula, str):
        return False
    return cedula.isdigit() and len(cedula) == 10


def calcular_edad(fecha_nacimiento):
    hoy = datetime.today()
    edad = hoy.year - fecha_nacimiento.year
    if (hoy.month, hoy.day) < (fecha_nacimiento.month, fecha_nacimiento.day):
        edad -= 1
    return edad


def validar_campos_requeridos(registro):
    errores = []

    required_fields = [
        "customer_id",
        "nombre",
        "apellido",
        "cedula",
        "fecha_nacimiento",
        "email",
        "direccion",
        "telefono",
        "fecha_creacion",
        "estado_cliente"
    ]

    for campo in required_fields:
        if campo not in registro or registro[campo] in [None, ""]:
            errores.append({
                "error_type": "schema",
                "rule": "campo_requerido",
                "field": campo,
                "message": "Campo requerido nulo o vacío"
            })

    return errores


def validar_schema(registro):
    errores = []

    if not validar_fecha_nacimiento(registro.get("fecha_nacimiento")):
        errores.append({
            "error_type": "schema",
            "rule": "fecha_nacimiento_formato",
            "field": "fecha_nacimiento",
            "message": "Formato inválido. Esperado: dd-mm-yyyy"
        })

    if not validar_fecha_creacion(registro.get("fecha_creacion")):
        errores.append({
            "error_type": "schema",
            "rule": "fecha_creacion_formato",
            "field": "fecha_creacion",
            "message": "Formato inválido. Esperado: dd/mm/yyyy HH:MM:SS"
        })

    if not validar_email(registro.get("email")):
        errores.append({
            "error_type": "schema",
            "rule": "email_formato",
            "field": "email",
            "message": "Email inválido"
        })

    if not validar_telefono(registro.get("telefono")):
        errores.append({
            "error_type": "schema",
            "rule": "telefono_formato",
            "field": "telefono",
            "message": "Teléfono inválido"
        })

    if not validar_cedula_basica(registro.get("cedula")):
        errores.append({
            "error_type": "schema",
            "rule": "cedula_formato",
            "field": "cedula",
            "message": "Cédula inválida"
        })

    return errores


def validar_domain(registro):
    errores = []

    if registro.get("estado_cliente") not in ["Activo", "Inactivo"]:
        errores.append({
            "error_type": "domain",
            "rule": "estado_cliente_valido",
            "field": "estado_cliente",
            "message": "Valor fuera del dominio permitido"
        })

    return errores


def validar_business(registro):
    errores = []

    fecha_nacimiento = registro.get("fecha_nacimiento")
    fecha_creacion = registro.get("fecha_creacion")
    estado_cliente = registro.get("estado_cliente")

    if validar_fecha_nacimiento(fecha_nacimiento):
        fecha_nac = datetime.strptime(fecha_nacimiento, "%d-%m-%Y")
        edad = calcular_edad(fecha_nac)

        if edad < 18:
            errores.append({
                "error_type": "business",
                "rule": "edad_minima",
                "field": "fecha_nacimiento",
                "message": "Cliente menor de 18 años"
            })

    if estado_cliente == "Inactivo" and validar_fecha_creacion(fecha_creacion):
        fecha_crea = datetime.strptime(fecha_creacion, "%d/%m/%Y %H:%M:%S")
        dias = (datetime.today() - fecha_crea).days

        if dias < 180:
            errores.append({
                "error_type": "business",
                "rule": "inactivo_antiguedad_minima",
                "field": "fecha_creacion",
                "message": "Cliente inactivo con antigüedad menor a 6 meses"
            })

    return errores


def validar_registro(registro):
    errores = []

    errores_required = validar_campos_requeridos(registro)
    errores.extend(errores_required)

    errores.extend(validar_schema(registro))
    errores.extend(validar_domain(registro))
    errores.extend(validar_business(registro))

    return errores


def validar_duplicados(data):
    errores = []
    seen = {}

    for i, row in enumerate(data):
        customer_id = row.get("customer_id")

        if customer_id in seen:
            errores.append({
                "row_index": i,
                "customer_id": customer_id,
                "error_type": "dup",
                "rule": "customer_id_unico",
                "field": "customer_id",
                "message": "Customer_id duplicado"
            })
        else:
            seen[customer_id] = i

    return errores


def build_report(data, errores_totales):
    errores_por_regla = {}
    errores_por_tipo = {}

    for err in errores_totales:
        regla = err["rule"]
        tipo = err["error_type"]

        errores_por_regla[regla] = errores_por_regla.get(regla, 0) + 1
        errores_por_tipo[tipo] = errores_por_tipo.get(tipo, 0) + 1

    registros_con_error = len(set([e["row_index"] for e in errores_totales]))
    total_registros = len(data)

    if total_registros > 0:
        porcentaje_cumplimiento = ((total_registros - registros_con_error) / float(total_registros)) * 100
    else:
        porcentaje_cumplimiento = 0.0

    reporte = {
        "total_registros": total_registros,
        "reglas_evaluadas": 10,
        "errores_totales": len(errores_totales),
        "errores_por_tipo": errores_por_tipo,
        "errores_por_regla": errores_por_regla,
        "porcentaje_cumplimiento": round(porcentaje_cumplimiento, 2),
        "muestras_errores": errores_totales[:10]
    }

    return reporte


def validar_dataset(data):
    errores_totales = []

    for i, row in enumerate(data):
        errores = validar_registro(row)

        for err in errores:
            err["row_index"] = i
            err["customer_id"] = row.get("customer_id")
            errores_totales.append(err)

    errores_dup = validar_duplicados(data)
    errores_totales.extend(errores_dup)

    reporte = build_report(data, errores_totales)

    return reporte, errores_totales


def main():
    create_dir(OUTPUT_REPORT_DIR)

    logger.info("Leyendo dataset: %s" % INPUT_JSON)
    data = load_json(INPUT_JSON)

    reporte, errores = validar_dataset(data)

    report_json_name = generar_nombre_archivo("report", seed, "json")
    errors_json_name = generar_nombre_archivo("errors", seed, "json")
    errors_csv_name = generar_nombre_archivo("errors", seed, "csv")

    report_json_path = os.path.join(OUTPUT_REPORT_DIR, report_json_name)
    errors_json_path = os.path.join(OUTPUT_REPORT_DIR, errors_json_name)
    errors_csv_path = os.path.join(OUTPUT_REPORT_DIR, errors_csv_name)

    save_json(report_json_path, reporte)
    save_json(errors_json_path, errores)
    save_csv(errors_csv_path, errores)

    logger.info("Reporte generado: %s" % report_json_path)

if __name__ == '__main__':
    main()