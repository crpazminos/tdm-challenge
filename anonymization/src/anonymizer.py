# -*- coding: utf-8 -*-
# anonymization/src/anonymizer.py

import os
import re
import json
import shutil
import sqlite3
import hashlib
from collections import OrderedDict
from utils.config import load_config
from utils.logging import setup_logger

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
DB_DIR = os.path.join(BASE_DIR, 'data')
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'shared', 'config', 'config.yaml')

config = load_config(CONFIG_PATH)

source_db_path = os.path.join(DB_DIR, config['database']['name'])
scan_report_path = os.path.join(OUTPUT_DIR, config['scanner']['file_name'])
output_db_path = os.path.join(DB_DIR, config['anonymizer']['db_name'])
anonymization_report_path = os.path.join(OUTPUT_DIR, config['anonymizer']['file_name'])
seed = config['anonymizer']['seed']

scan_report_abs_path = os.path.join(PROJECT_ROOT, scan_report_path)
source_db_abs_path = os.path.join(PROJECT_ROOT, source_db_path)
output_db_abs_path = os.path.join(PROJECT_ROOT, output_db_path)
anonymization_report_abs_path = os.path.join(PROJECT_ROOT, anonymization_report_path)

log_path = os.path.join('anonymization', 'logs')
logger = setup_logger(PROJECT_ROOT, log_path, 'anonymizer')

def create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def normalize_numeric(value):
    try:
        return re.sub(r'[^0-9]', '', str(value))
    except Exception:
        return ''

def load_json(path):
    with open(path, 'r') as handler:
        return json.load(handler)

def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA foreign_keys = ON;')
    logger.info("Conexión a la BD: %s" % db_path)
    return conn

def deterministic_hash(seed, pii_type, original_value):
    raw = u"{0}|{1}|{2}".format(seed, pii_type, original_value)
    if not isinstance(raw, bytes):
        raw = raw.encode('utf-8')
    return hashlib.sha256(raw).hexdigest()

def generate_digits_from_hash(hash_hex, length):
    digits = ''
    index = 0

    while len(digits) < length:
        chunk = hash_hex[index:index + 2]
        if not chunk:
            hash_hex = hashlib.sha256(hash_hex.encode('utf-8')).hexdigest()
            index = 0
            continue

        number = int(chunk, 16) % 10
        digits += str(number)
        index += 2

    return digits[:length]

def anonymize_email(value, seed):
    original = str(value).strip()
    hash_hex = deterministic_hash(seed, 'EMAIL', original)

    local_part = hash_hex[:10]
    domain_part = hash_hex[10:18]

    return "{0}@{1}.com".format(local_part, domain_part)

def anonymize_phone(value, seed):
    original = str(value).strip()
    hash_hex = deterministic_hash(seed, 'PHONE', original)

    suffix = generate_digits_from_hash(hash_hex, 8)
    return "09{0}".format(suffix)

def compute_cedula_check_digit(first_nine_digits):
    total = 0
    for idx in range(9):
        digit = int(first_nine_digits[idx])
        if idx % 2 == 0:
            digit = digit * 2
            if digit > 9:
                digit -= 9
        total += digit

    return '0' if total % 10 == 0 else str(10 - (total % 10))

def anonymize_cedula(value, seed):
    original = str(value).strip()
    hash_hex = deterministic_hash(seed, 'CEDULA', original)

    province_seed = int(hash_hex[0:2], 16) % 24 + 1
    province = str(province_seed).zfill(2)

    third_digit = str(int(hash_hex[2:4], 16) % 6)
    middle_digits = generate_digits_from_hash(hash_hex[4:], 6)

    first_nine = province + third_digit + middle_digits
    check_digit = compute_cedula_check_digit(first_nine)

    return first_nine + check_digit

def anonymize_ruc_natural(value, seed):
    cedula = anonymize_cedula(value, seed)
    return cedula + '001'

def anonymize_ruc_empresa(value, seed):
    original = str(value).strip()
    hash_hex = deterministic_hash(seed, 'RUC_EMPRESA', original)

    province_seed = int(hash_hex[0:2], 16) % 24 + 1
    province = str(province_seed).zfill(2)

    third_digit = '9'
    body = generate_digits_from_hash(hash_hex[4:], 9)

    return province + third_digit + body[:7] + '001'

def anonymize_value(value, pii_type, seed):
    if value is None:
        return None
    if pii_type == 'EMAIL':
        return anonymize_email(value, seed)
    if pii_type == 'PHONE':
        return anonymize_phone(value, seed)
    if pii_type == 'CEDULA':
        return anonymize_cedula(value, seed)
    if pii_type == 'RUC_NATURAL':
        return anonymize_ruc_natural(value, seed)
    if pii_type == 'RUC_EMPRESA':
        return anonymize_ruc_empresa(value, seed)

    return value

def build_sensitive_column_map(scan_report):
    sensitive_map = OrderedDict()

    for table in scan_report.get('tables', []):
        table_name = table['table_name']
        columns_map = OrderedDict()

        for column in table.get('columns', []):
            if column.get('should_anonymize'):
                detected_type = column.get('detected_type')
                column_name = column.get('column_name')

                if detected_type:
                    columns_map[column_name] = detected_type

        if columns_map:
            sensitive_map[table_name] = columns_map

    return sensitive_map

def get_primary_key_column(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info({0})".format(table_name))
    for row in cursor.fetchall():
        if row[5] == 1:
            return row[1]
    return None


def anonymize_table(conn, table_name, sensitive_columns, seed):
    pk_column = get_primary_key_column(conn, table_name)
    if not pk_column:
        logger.info("Primary key no se encontro: {0}".format(table_name))

    cursor = conn.cursor()

    select_sql = "SELECT * FROM {0}".format(table_name)
    cursor.execute(select_sql)

    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    updates_count = 0

    for row in rows:
        row_dict = dict(zip(column_names, row))
        pk_value = row_dict[pk_column]

        set_clauses = []
        params = []

        for column_name, pii_type in sensitive_columns.items():
            original_value = row_dict.get(column_name)
            anonymized_value = anonymize_value(original_value, pii_type, seed)

            if anonymized_value != original_value:
                set_clauses.append("{0} = ?".format(column_name))
                params.append(anonymized_value)

        if set_clauses:
            params.append(pk_value)
            update_sql = "UPDATE {0} SET {1} WHERE {2} = ?".format(
                table_name,
                ", ".join(set_clauses),
                pk_column
            )
            cursor.execute(update_sql, params)
            updates_count += 1

    conn.commit()
    return updates_count

def copy_database(source_path, target_path):
    create_dir(os.path.dirname(target_path))
    shutil.copy2(source_path, target_path)

def build_anonymization_report(sensitive_map, updated_tables):
    report = OrderedDict()
    report['tables'] = []

    for table_name, columns in sensitive_map.items():
        report['tables'].append(OrderedDict([
            ('table_name', table_name),
            ('updated_rows', updated_tables.get(table_name, 0)),
            ('anonymized_columns', columns)
        ]))

    return report

def save_json(data, path):
    create_dir(os.path.dirname(path))
    with open(path, 'w') as handler:
        json.dump(data, handler, indent=4)

def print_summary(report):
    logger.info("=" * 80)
    logger.info("ANONYMIZATION SUMMARY")
    logger.info("=" * 80)

    for table in report['tables']:
        logger.info("Table: {0}".format(table['table_name']))
        logger.info("  Updated rows: {0}".format(table['updated_rows']))
        logger.info("  Columns:")
        for column_name, pii_type in table['anonymized_columns'].items():
            logger.info("    - {0}: {1}".format(column_name, pii_type))
        logger.info("")

def main():
    scan_report = load_json(scan_report_abs_path)
    sensitive_map = build_sensitive_column_map(scan_report)

    copy_database(source_db_abs_path, output_db_abs_path)

    conn = None
    try:
        conn = get_connection(output_db_abs_path)
        updated_tables = OrderedDict()

        for table_name, sensitive_columns in sensitive_map.items():
            updated_rows = anonymize_table(
                conn=conn,
                table_name=table_name,
                sensitive_columns=sensitive_columns,
                seed=seed
            )
            updated_tables[table_name] = updated_rows

        report = build_anonymization_report(sensitive_map, updated_tables)
        save_json(report, anonymization_report_abs_path)

        print_summary(report)
        logger.info("Anonimizacion guardada en: {0}".format(output_db_abs_path))
        logger.info("Anonimizacion reporte guardado en: {0}".format(anonymization_report_abs_path))

    except Exception as exc:
        logger.info("Error al anonimizar: {0}".format(str(exc)))
        raise
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()