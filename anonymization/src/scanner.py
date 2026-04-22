# -*- coding: utf-8 -*-
# anonymization/src/scanner.py

import os
import re
import json
import sqlite3
from utils.config import load_config
from collections import OrderedDict
from utils.logging import setup_logger

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'shared', 'config', 'config.yaml')

config = load_config(CONFIG_PATH)

DB_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(DB_DIR, config['database']['name'])

DEFAULT_THRESHOLD = config['scanner']['threshold']
DEFAULT_SAMPLE_SIZE = config['scanner']['sample_size']
FILE_NAME = config['scanner']['file_name']

OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
REPORT_PATH = os.path.join(OUTPUT_DIR, FILE_NAME)

log_path = os.path.join('anonymization', 'logs')
logger = setup_logger(PROJECT_ROOT, log_path, 'scanner')

def create_dir(output_dir=OUTPUT_DIR):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

def get_connection(db_file=DB_PATH):
    try:
        conn = sqlite3.connect(db_file)
        conn.execute('pragma foreign_keys=on')
        logger.info("Conexión a la BD: %s" % db_file)
        return conn
    except sqlite3.Error as e:
        print(e)

def get_table_name(conn):
    try:
        cursor = conn.cursor()
        qry = """
                       SELECT name
                       FROM sqlite_master
                       WHERE type = 'table'
                         AND name NOT LIKE 'sqlite_%'
                       ORDER BY name;
                       """
        cursor.execute(qry)

        logger.info("Listado de tablas")
        logger.info(qry)

        return [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(e)

def get_table_columns(conn, table_name):
    try:
        cursor = conn.cursor()
        qry = "PRAGMA table_info(%s)" % table_name
        cursor.execute(qry)

        logger.info("Columnas")
        logger.info(qry)

        columns = []
        for row in cursor.fetchall():
            columns.append({
                'cid': row[0],
                'name': row[1],
                'type': row[2],
                'notnull': row[3],
                'default_value': row[4],
                'is_pk': row[5]
            })
        return columns
    except sqlite3.Error as e:
        print(e)
    except Exception as e:
        print(e)

def get_column_sample(conn, table_name, column_name, sample_size):
    try:
        cursor = conn.cursor()
        query = """
            SELECT {0}
            FROM {1}
            WHERE {0} IS NOT NULL
              AND TRIM(CAST({0} AS TEXT)) <> ''
            LIMIT {2};
        """.format(column_name, table_name, sample_size)

        cursor.execute(query)
        rows = cursor.fetchall()

        logger.info("Muestra")
        logger.info(query)

        values = []
        for row in rows:
            values.append(str(row[0]).strip())
        return values
    except sqlite3.Error as e:
        print(e)
    except Exception as e:
        print(e)


def normalize_numeric(value):
    return re.sub('[^0-9]', '', str(value))


def is_email(value):
    pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    return re.match(pattern, value) is not None


def is_phone_ec(value):
    numeric = normalize_numeric(value)
    return len(numeric) == 10 and numeric.startswith('09')


def is_valid_ci(value):
    numeric = normalize_numeric(value)

    if len(numeric) != 10:
        return False

    province = int(numeric[0:2])
    third_digit = int(numeric[2])

    if province < 1 or province > 24:
        return False

    if third_digit >= 6:
        return False

    total = 0
    for idx in range(9):
        digit = int(numeric[idx])
        if idx % 2 == 0:
            digit = digit * 2
            if digit > 9:
                digit -= 9
        total += digit

    verifier = int(numeric[9])
    expected = 0 if total % 10 == 0 else 10 - (total % 10)
    return verifier == expected

def is_ruc_natural(value):
    numeric = normalize_numeric(value)

    if len(numeric) != 13:
        return False

    if not numeric.endswith('001'):
        return False

    return is_valid_ci(numeric[:10])

def is_ruc_empresa(value):
    numeric = normalize_numeric(value)

    if len(numeric) != 13:
        return False

    province = int(numeric[0:2])
    third_digit = int(numeric[2])

    if province < 1 or province > 24:
        return False

    if third_digit not in (6, 9):
        return False

    if not numeric.endswith('001'):
        return False

    return True

def evaluate_value_against_types(value):
    return OrderedDict([
        ('EMAIL', is_email(value)),
        ('PHONE', is_phone_ec(value)),
        ('CEDULA', is_valid_ci(value)),
        ('RUC_NATURAL', is_ruc_natural(value)),
        ('RUC_EMPRESA', is_ruc_empresa(value))
    ])

def analyze_column(values, threshold):
    result = OrderedDict()
    result['sample_size'] = len(values)
    result['matches'] = OrderedDict([
        ('EMAIL', 0),
        ('PHONE', 0),
        ('CEDULA', 0),
        ('RUC_NATURAL', 0),
        ('RUC_EMPRESA', 0)
    ])

    result['probabilities'] = OrderedDict([
        ('EMAIL', 0.0),
        ('PHONE', 0.0),
        ('CEDULA', 0.0),
        ('RUC_NATURAL', 0.0),
        ('RUC_EMPRESA', 0.0)
    ])

    result['detected_type'] = None
    result['detected_probability'] = 0.0
    result['should_anonymize'] = False

    if not values:
        return result

    for value in values:
        evaluation = evaluate_value_against_types(value)
        for pii, matched in evaluation.items():
            if matched:
                result['matches'][pii] += 1

    total = float(len(values))

    max_type = None
    max_probability = 0.0

    for pii in result['matches']:
        probability = result['matches'][pii] / total
        result['probabilities'][pii] = round(probability, 4)

        if probability > max_probability:
            max_probability = probability
            max_type = pii

    result['detected_type'] = max_type
    result['detected_probability'] = round(max_probability, 4)
    result['should_anonymize'] = max_probability >= threshold

    return result

def scan_database(conn, threshold, sample_size):
    scan_result = OrderedDict()
    scan_result['threshold'] = threshold
    scan_result['sample_size'] = sample_size
    scan_result['tables'] = []

    tables = get_table_name(conn)

    for table_name in tables:
        table_info = OrderedDict()
        table_info['table_name'] = table_name
        table_info['columns'] = []

        columns = get_table_columns(conn, table_name)

        for column in columns:
            column_name = column['name']
            values = get_column_sample(conn, table_name, column_name, sample_size)
            analysis = analyze_column(values, threshold)

            column_result = OrderedDict()
            column_result['column_name'] = column_name
            column_result['data_type'] = column['type']
            column_result['is_primary_key'] = bool(column['is_pk'])
            column_result['sample_size'] = analysis['sample_size']
            column_result['matches'] = analysis['matches']
            column_result['probabilities'] = analysis['probabilities']
            column_result['detected_type'] = analysis['detected_type']
            column_result['detected_probability'] = analysis['detected_probability']
            column_result['should_anonymize'] = analysis['should_anonymize']

            table_info['columns'].append(column_result)

        scan_result['tables'].append(table_info)

    return scan_result

def save_report(report, path):
    create_dir(os.path.dirname(path))
    with open(path, 'w') as handler:
        json.dump(report, handler, indent=4)

def print_summary(report):
    logger.info("=" * 80)
    logger.info("SENSITIVE DATA SCAN SUMMARY")
    logger.info("=" * 80)
    logger.info("Threshold: {0}".format(report['threshold']))
    logger.info("Sample size: {0}".format(report['sample_size']))
    logger.info("")

    for table in report['tables']:
        logger.info("Table: {0}".format(table['table_name']))
        for column in table['columns']:
            logger.info(
                "  - Column: {0:20} | Type: {1:12} | Prob: {2:>6} | Anonymize: {3}".format(
                    column['column_name'],
                    str(column['detected_type']),
                    str(column['detected_probability']),
                    str(column['should_anonymize'])
                )
            )
        logger.info("")


def main():
    conn = None
    try:
        conn = get_connection(DB_PATH)
        report = scan_database(conn=conn, threshold=DEFAULT_THRESHOLD, sample_size=DEFAULT_SAMPLE_SIZE)
        save_report(report, REPORT_PATH)
        print_summary(report)
        logger.info('Reporte almacenado en: %s' % REPORT_PATH)
    except Exception as e:
        logger.info('Error en el escaneo: %s' % e)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()



