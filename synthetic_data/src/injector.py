# -*- coding: utf-8 -*-
# synthetic_data/src/injector.py

import os
import random
import sqlite3
import json
from datetime import datetime, timedelta
from utils.config import load_config
from utils.logging import setup_logger

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

CONFIG_PATH = os.path.join(PROJECT_ROOT, 'shared', 'config', 'config.yaml')

config = load_config(CONFIG_PATH)

log_path = os.path.join('synthetic_data', 'logs')
logger = setup_logger(PROJECT_ROOT, log_path, 'injector')

sample_size = config['inyector']['sample_size']
error_rate = config['inyector']['error_rate']
seed = config['inyector']['seed']
report_name = config['inyector']['report_name']
data_name = config['inyector']['data_name']

OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

DB_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DB_DIR, config['generator']['db_name'])

def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        conn.execute('pragma foreign_keys=on')
        logger.info("Conexión a la BD: %s" % db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

def guardar_json(data, file_name, path):
    path_file = os.path.join(path, file_name)

    with open(path_file, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def create_dir(db_dir=DB_DIR):
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)


def create_connection(db_file=DB_PATH):
    try:
        conn = sqlite3.connect(db_file)
        conn.execute('pragma foreign_keys=on')
        logger.info("Conexión a la BD: %s" % db_file)
        return conn
    except sqlite3.Error as e:
        print(e)


def get_data_sample(conn, size):
    try:
        cursor = conn.cursor()
        qry = "SELECT * FROM clientes LIMIT %s" % size
        cursor.execute(qry)

        logger.info("Muestra de datos: %s" % size)

        columnas = [col[0] for col in cursor.description]

        data = []
        for row in cursor.fetchall():
            data.append(dict(zip(columnas, row)))

        conn.commit()
        return data
    except Exception as e:
        print(e)


def calcular_total_errores(total_registros, error_rate):
    total = int(round(total_registros * error_rate))

    if error_rate > 0 and total == 0:
        total = 1

    return total


def seleccionar_indices(total_registros, total_errores, seed):
    random.seed(seed)
    universo = list(range(total_registros))
    total_errores = min(total_errores, total_registros)
    return random.sample(universo, total_errores)


def inject_schema_error(registro, seed=None):
    opciones = [
        "fecha_nacimiento",
        "email",
        "telefono",
        "cedula"
    ]

    campo = random.choice(opciones)

    if campo == "fecha_nacimiento":
        registro["fecha_nacimiento"] = "1900/12/31"
    elif campo == "email":
        registro["email"] = "correo_invalido"
    elif campo == "telefono":
        registro["telefono"] = "abc247"
    elif campo == "cedula":
        registro["cedula"] = "789"

    return registro, campo


def inject_domain_error(registro, seed=None):
    registro["estado_cliente"] = "Suspendido"
    return registro, "estado_cliente"


def inject_dup_error(data, indice_destino, indice_origen):
    data[indice_destino]["customer_id"] = data[indice_origen]["customer_id"]
    return data, "customer_id"


def inject_business_error(registro, seed=None):
    opciones = [
        "edad_menor_18",
        "inactivo_fecha_reciente"
    ]

    tipo = random.choice(opciones)

    if tipo == "edad_menor_18":
        fecha = datetime.today() - timedelta(days=(10 * 365))
        registro["fecha_nacimiento"] = fecha.strftime("%d-%m-%Y")
        return registro, "fecha_nacimiento"

    if tipo == "inactivo_fecha_reciente":
        registro["estado_cliente"] = "Inactivo"
        fecha = datetime.today() - timedelta(days=30)
        registro["fecha_creacion"] = fecha.strftime("%d/%m/%Y %H:%M:%S")
        return registro, "fecha_creacion"

    return registro, None


def inyectar_fallas(data, error_rate, seed):
    random.seed(seed)

    total_registros = len(data)
    total_errores = calcular_total_errores(total_registros, error_rate)

    tipos = ["schema", "domain", "dup", "business"]
    indices = seleccionar_indices(total_registros, total_errores, seed)

    errores_inyectados = []

    for i, idx in enumerate(indices):
        tipo_error = tipos[i % len(tipos)]

        if tipo_error == "schema":
            data[idx], campo = inject_schema_error(data[idx])
            errores_inyectados.append({
                "row_index": idx,
                "customer_id": data[idx]["customer_id"],
                "error_type": "schema",
                "field": campo
            })

        elif tipo_error == "domain":
            data[idx], campo = inject_domain_error(data[idx])
            errores_inyectados.append({
                "row_index": idx,
                "customer_id": data[idx]["customer_id"],
                "error_type": "domain",
                "field": campo
            })

        elif tipo_error == "business":
            data[idx], campo = inject_business_error(data[idx])
            errores_inyectados.append({
                "row_index": idx,
                "customer_id": data[idx]["customer_id"],
                "error_type": "business",
                "field": campo
            })

        elif tipo_error == "dup":
            indice_origen = 0 if idx != 0 else 1
            data, campo = inject_dup_error(data, idx, indice_origen)
            errores_inyectados.append({
                "row_index": idx,
                "customer_id": data[idx]["customer_id"],
                "error_type": "dup",
                "field": campo
            })

    return data, errores_inyectados


def main():
    conn = None
    create_dir(DB_DIR)
    try:
        conn = create_connection(DB_PATH)
        data = get_data_sample(conn, sample_size)

        logger.info("Aplicando inyección")
        data, errores = inyectar_fallas(data, error_rate, seed)

        logger.info("Generando reporte: %s" % OUTPUT_DIR)
        guardar_json(errores, report_name, OUTPUT_DIR)
        logger.info("Generando data inyectada: %s" % DB_DIR)
        guardar_json(data, data_name, DB_DIR)

    except Exception as e:
        logger.error(e)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
