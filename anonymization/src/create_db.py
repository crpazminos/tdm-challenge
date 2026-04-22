# -*- coding: utf-8 -*-
# anonymization/src/create_db.py

import os
import sqlite3
from utils.config import load_config
from utils.logging import setup_logger

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'shared', 'config', 'config.yaml')

config = load_config(CONFIG_PATH)

DB_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DB_DIR, config['database']['name'])

log_path = os.path.join('anonymization', 'logs')
logger = setup_logger(PROJECT_ROOT, log_path, 'create_db')

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
        qry_customers = 'DROP TABLE IF EXISTS customers;'
        qry_invoices = 'DROP TABLE IF EXISTS invoices;'
        qry_notifications = 'DROP TABLE IF EXISTS notifications;'
        cursor.execute(qry_customers)
        cursor.execute(qry_invoices)
        cursor.execute(qry_notifications)

        logger.info("Depuración de tablas")
        logger.info(qry_customers)
        logger.info(qry_invoices)
        logger.info(qry_notifications)

        conn.commit()
    except sqlite3.Error as e:
        print(e)

def create_tables(conn):
    try:
        cursor = conn.cursor()

        qry_customers = """
            CREATE TABLE customers (
                customer_id      INTEGER PRIMARY KEY,
                customer_code    TEXT NOT NULL UNIQUE,
                full_name        TEXT NOT NULL,
                document_type    TEXT NOT NULL,
                document_number  TEXT NOT NULL,
                email            TEXT,
                phone            TEXT,
                birth_date       TEXT,
                city             TEXT,
                created_at       TEXT NOT NULL
            );
        """
        cursor.execute(qry_customers)

        qry_invoices = """
            CREATE TABLE invoices (
                invoice_id          INTEGER PRIMARY KEY,
                invoice_number      TEXT NOT NULL UNIQUE,
                customer_id         INTEGER NOT NULL,
                billing_document    TEXT NOT NULL,
                billing_email       TEXT,
                billing_phone       TEXT,
                subtotal            REAL NOT NULL,
                tax                 REAL NOT NULL,
                total               REAL NOT NULL,
                invoice_date        TEXT NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
        """
        cursor.execute(qry_invoices)

        qry_notifications = """
            CREATE TABLE notifications (
                notification_id     INTEGER PRIMARY KEY,
                customer_id         INTEGER NOT NULL,
                channel             TEXT NOT NULL,
                destination_value   TEXT NOT NULL,
                message_subject     TEXT,
                message_body        TEXT,
                status              TEXT NOT NULL,
                sent_at             TEXT,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            );
        """
        cursor.execute(qry_notifications)

        logger.info("Creación de tablas")
        logger.info(qry_customers)
        logger.info(qry_invoices)
        logger.info(qry_notifications)

        conn.commit()
    except sqlite3.Error as e:
        print(e)

def main():
    conn = None
    create_dir(DB_DIR)
    try:
        conn = create_connection(db_file=DB_PATH)
        drop_tables(conn)
        create_tables(conn)
        logger.info("Base de datos creada exitosamente: %s" % DB_PATH)
    except Exception as e:
        logger.info("Error al crear al base de datos: %s" % e)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()


