# -*- coding: utf-8 -*-
# synthetic_data/src/generator.py

import os
import random
import json
import sqlite3
from datetime import datetime, timedelta
from faker import Faker
from utils.config import load_config
from utils.logging import setup_logger

fake = Faker("es_ES")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'shared', 'config', 'config.yaml')

config = load_config(CONFIG_PATH)

log_path = os.path.join('synthetic_data', 'logs')
logger = setup_logger(PROJECT_ROOT, log_path, 'generator')

DB_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DB_DIR, config['generator']['db_name'])


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


def drop_tables(conn):
    try:
        cursor = conn.cursor()
        qry = 'DROP TABLE IF EXISTS clientes;'
        cursor.execute(qry)

        logger.info("Depuración de tablas")
        logger.info(qry)

        conn.commit()
    except sqlite3.Error as e:
        print(e)


def create_table(conn, data):
    try:
        cursor = conn.cursor()

        qry = """
              CREATE TABLE clientes
              (
                  customer_id      TEXT PRIMARY KEY,
                  nombre           TEXT NOT NULL,
                  apellido         TEXT NOT NULL,
                  cedula           TEXT NOT NULL,
                  fecha_nacimiento TEXT NOT NULL,
                  email            TEXT,
                  direccion        TEXT,
                  telefono         TEXT,
                  fecha_creacion   TEXT,
                  estado_cliente   TEXT NOT NULL
              ); 
              """
        cursor.execute(qry)

        logger.info("Creación de la tabla")
        logger.info(qry)

        rows = []
        for row in data:
            rows.append((
                row['customer_id'],
                row['nombre'],
                row['apellido'],
                row['cedula'],
                row['fecha_nacimiento'],
                row['email'],
                row['direccion'],
                row['telefono'],
                row['fecha_creacion'],
                row['estado_cliente']
            ))


        query = """
                INSERT INTO clientes (customer_id,
                                       nombre,
                                       apellido,
                                       cedula,
                                       fecha_nacimiento,
                                       email,
                                       direccion,
                                       telefono,
                                       fecha_creacion,
                                       estado_cliente)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """

        cursor.executemany(query, rows)
        conn.commit()

        logger.info("Insert registros en clientes")
        logger.info(query)
        logger.info("Registros insertados clientes: %s" % len(rows))

    except sqlite3.Error as e:
        print(e)


def guardar_json(data, seed, path):
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M")
    file_name = "clientes_{0}_seed{1}.json".format(timestamp, seed)
    path_file = os.path.join(path, file_name)

    with open(path_file, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def generar_customer_id(indice):
    return "Cus{0:03d}".format(indice)


def generar_telefono():
    return "09{0}".format("".join([str(random.randint(0, 9)) for _ in range(8)]))


def generar_fecha_nacimiento():
    hoy = datetime.today()
    edad = random.randint(18, 90)
    dias_extra = random.randint(0, 364)
    fecha = hoy - timedelta(days=(edad * 365 + dias_extra))
    return fecha.strftime("%d-%m-%Y")


def generar_fecha_creacion(estado_cliente):
    hoy = datetime.today()

    if estado_cliente == "Inactivo":
        dias_atras = random.randint(180, 1000)
    else:
        dias_atras = random.randint(0, 1000)

    segundos_atras = random.randint(0, 86400)
    return hoy - timedelta(days=dias_atras, seconds=segundos_atras)


def generar_email(nombre, apellido):
    dominio = random.choice(["test.com", "mail.com", "demo.ec"])
    return "{0}.{1}@{2}".format(nombre.lower(), apellido.lower(), dominio)


def generar_cedula_sintetica():
    provincia = random.randint(1, 24)
    tercer_digito = random.randint(0, 5)
    base = "{0:02d}{1}".format(provincia, tercer_digito)

    for _ in range(6):
        base += str(random.randint(0, 9))

    coeficientes = [2, 1, 2, 1, 2, 1, 2, 1, 2]
    total = 0

    for i in range(9):
        valor = int(base[i]) * coeficientes[i]
        if valor >= 10:
            valor -= 9
        total += valor

    verificador = 10 - (total % 10)
    if verificador == 10:
        verificador = 0

    return base + str(verificador)


def generar_cliente(indice):
    nombre = fake.first_name()
    apellido = fake.last_name()
    estado_cliente = random.choice(["Activo", "Inactivo"])

    fecha_nacimiento = generar_fecha_nacimiento()
    fecha_creacion = generar_fecha_creacion(estado_cliente)

    cliente = {
        "customer_id": generar_customer_id(indice),
        "nombre": nombre,
        "apellido": apellido,
        "cedula": generar_cedula_sintetica(),
        "fecha_nacimiento": fecha_nacimiento,
        "email": generar_email(nombre, apellido),
        "direccion": fake.address().replace("\n", " "),
        "telefono": generar_telefono(),
        "fecha_creacion": fecha_creacion.strftime("%d/%m/%Y %H:%M:%S"),
        "estado_cliente": estado_cliente
    }

    return cliente


def generar_clientes(total_registros, seed=42):
    random.seed(seed)
    fake.seed_instance(seed)

    clientes = []
    for i in range(1, total_registros + 1):
        clientes.append(generar_cliente(i))

    return clientes


def main():
    conn = None
    create_dir(DB_DIR)
    try:
        conn = create_connection(DB_PATH)
        drop_tables(conn)

        records = config['generator']['records']
        seed = config['generator']['seed']

        logger.info("Generando clientes")
        data = generar_clientes(records, seed)
        logger.info("Numero de clientes generados: %s" % len(data))

        create_table(conn, data)
        guardar_json(data,seed,OUTPUT_DIR)

        logger.info("Registros sintéticos almacenados en: %s" % DB_PATH)

    except Exception as e:
        logger.info(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
