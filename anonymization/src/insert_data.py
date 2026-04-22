# -*- coding: utf-8 -*-
# anonymization/src/insert_data.py

import os
import json
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
logger = setup_logger(PROJECT_ROOT, log_path, 'insert_data')

IMPUT_PATH = os.path.join(BASE_DIR, 'input')
CUSTOMERS_PATH = os.path.join(IMPUT_PATH, 'customers.json')
INVOICES_PATH = os.path.join(IMPUT_PATH, 'invoices.json')
NOTIFICATIONS_PATH = os.path.join(IMPUT_PATH, 'notifications.json')


def get_connection(db_file=DB_PATH):
    try:
        conn = sqlite3.connect(db_file)
        conn.execute('pragma foreign_keys=on')
        logger.info("Conexión a la BD: %s" % db_file)
        return conn
    except sqlite3.Error as e:
        print(e)


def load_json(file_path):
    with open(file_path, 'r') as handler:
        return json.load(handler)


def insert_customers(conn, customers_data):
    try:
        rows = []
        for row in customers_data:
            rows.append((
                row['customer_id'],
                row['customer_code'],
                row['full_name'],
                row['document_type'],
                row['document_number'],
                row['email'],
                row['phone'],
                row['birth_date'],
                row['city'],
                row['created_at']
            ))

        query = """
                INSERT INTO customers (customer_id,
                                       customer_code,
                                       full_name,
                                       document_type,
                                       document_number,
                                       email,
                                       phone,
                                       birth_date,
                                       city,
                                       created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """

        cursor = conn.cursor()
        cursor.executemany(query, rows)
        conn.commit()

        logger.info("Insert registros en customers")
        logger.info(query)
        logger.info("Registros insertados customers: %s" % len(rows))
    except sqlite3.Error as e:
        logger.info(e)


def insert_invoices(conn, invoices_data):
    try:
        rows = []
        for row in invoices_data:
            rows.append((
                row['invoice_id'],
                row['invoice_number'],
                row['customer_id'],
                row['billing_document'],
                row['billing_email'],
                row['billing_phone'],
                row['subtotal'],
                row['tax'],
                row['total'],
                row['invoice_date']
            ))

        query = """
                INSERT INTO invoices (invoice_id,
                                      invoice_number,
                                      customer_id,
                                      billing_document,
                                      billing_email,
                                      billing_phone,
                                      subtotal,
                                      tax,
                                      total,
                                      invoice_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """

        cursor = conn.cursor()
        cursor.executemany(query, rows)
        conn.commit()
        logger.info("Insert registros en invoices")
        logger.info(query)
        logger.info("Registros insertados invoices: %s" % len(rows))
    except sqlite3.Error as e:
        logger.info(e)


def insert_notifications(conn, notifications_data):
    try:
        rows = []
        for row in notifications_data:
            rows.append((
                row['notification_id'],
                row['customer_id'],
                row['channel'],
                row['destination_value'],
                row.get('message_subject'),
                row['message_body'],
                row['status'],
                row.get('sent_at')
            ))

        query = """
                INSERT INTO notifications (notification_id, 
                                           customer_id, 
                                           channel, 
                                           destination_value, 
                                           message_subject, 
                                           message_body, 
                                           status, 
                                           sent_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """

        cursor = conn.cursor()
        cursor.executemany(query, rows)
        conn.commit()

        logger.info("Insert registros en notifications")
        logger.info(query)
        logger.info("Registros insertados notifications: %s" % len(rows))
    except sqlite3.Error as e:
        logger.info(e)


def main():
    conn = None
    try:
        conn = get_connection(DB_PATH)

        customers_data = load_json(CUSTOMERS_PATH)
        invoices_data = load_json(INVOICES_PATH)
        notifications_data = load_json(NOTIFICATIONS_PATH)

        insert_customers(conn, customers_data)
        insert_invoices(conn, invoices_data)
        insert_notifications(conn, notifications_data)
        logger.info("Datos insertado correctamente: %s" % DB_PATH)
    except Exception as e:
        logger.info("Error insertando datos: %s" % e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
