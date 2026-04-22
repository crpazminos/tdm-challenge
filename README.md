# TDM Challenge

## Estructura del proyecto

```
tdm-challenge/
├── anonymization/
│   ├── data/
│   │   ├── tdm.db
│   │   └── tdm_anonymizer.db
│   ├── input/
│   │   ├── customers.json
│   │   ├── invoices.json
│   │   └── notifications.json
│   ├── logs/
│   ├── output/
│   │   ├── anonymization_report.json
│   │   └── scan_report.json
│   └── src/
│       ├── create_db.py
│       ├── insert_data.py
│       ├── scanner.py
│       └── anonymizer.py
├── docs/
├── shared/
│   ├── config/
│   │   └── config.yaml
│   └── utils/
│       ├── config.py
│       └── logging.py
├── synthetic_data/
│   ├── data/
│   │   ├── data_injection.json
│   │   └── tdm_synthetic_data.db
│   ├── logs/
│   ├── output/
│   │   ├── clientes_yyyymmdd_hhmm_seednn.json
│   │   ├── errors_yyyymmdd_hhmm_seednn.csv
│   │   ├── errors_yyyymmdd_hhmm_seednn.json
│   │   ├── report_yyyymmdd_hhmm_seednn.json
│   │   └── report_injection.json
│   └── src/
│       ├── generator.py
│       ├── injector.py
│       └── validator.py
├── requirements.txt
└── README.md
```

## Configuración

Archivo: `shared/config/config.yaml`

```yaml
database:
  name: tdm.db

scanner:
  threshold: 0.9
  sample_size: 100
  file_name: scan_report.json

anonymizer:
  seed: tdm_crps
  file_name: anonymization_report.json
  db_name: tdm_anonymizer.db

logs:
  path_scan:
  path_inge:

generator:
  seed: 42
  records: 500
  db_name: tdm_synthetic_data.db

inyector:
  seed: 42
  error_rate: 0.05
  sample_size: 100
  report_name: report_injection.json
  data_name: data_injection.json
```

### Instalar dependencias 
```bash
pip install -r requirements.txt
```

---

## Logging
Archivo: `*.log`
```
anonymization/logs
synthetic_data/logs
```

# Parte 1: Data Scanning & Anonymization

## Objetivo

Implementar un componente de **Test Data Management (TDM)** que permita:
- Detectar datos sensibles (PII) en tablas relacionales  
- Clasificar el tipo de dato (email, teléfono, cédula, etc.)  
- Aplicar anonimización determinística  
- Preservar formato e integridad entre tablas  

---

## Ejecución del proyecto

### 1. Crear base de datos
```bash
python anonymization/src/create_db.py
```

### 2. Insertar datos
```bash
python anonymization/src/insert_data.py
```

### 3. Ejecutar scanner
```bash
python anonymization/src/scanner.py
```

**Salida:**
```
anonymization/output/scan_report.json
```

### 4. Ejecutar anonymizer
```bash
python anonymization/src/anonymizer.py
```

**Salida:**
```
anonymization/data/tdm_anonymizer.db
anonymization/output/anonymization_report.json
```
---

## Enfoque técnico

### Scanner

- Muestreo por columna  
- Validadores por tipo: `EMAIL`, `PHONE`, `CEDULA`, `RUC`  
- Probabilidad:

```text
probabilidad = coincidencias / total_muestra
```

- Selección del tipo dominante  
- Umbral configurable (default: `0.9`)

---

### Anonimización

Basada en hash determinístico:

```text
hash(seed + pii_type + valor_original)
```

Permite:

- Consistencia entre tablas  
- Reproducibilidad  
- Control por entorno

---

### Reglas

- **Email** → formato válido  
- **Teléfono** → 10 dígitos (prefijo `09`)  
- **Cédula** → válida (estructura + checksum)  
- **RUC natural** → cédula + `001`  
- **RUC empresa** → formato preservado

---

# Parte 2: Synthetic Data & Injection

## Objetivo

Diseñar un componente de **Test Data Management (TDM)** que permita:
- Generar datos sintéticos de clientes bajo un contrato definido
- Inyectar errores de forma controlada y determinística
- Validar calidad de datos según reglas de negocio
- Generar reportes trazables para QA

---

## Ejecución del proyecto

### 1. Generar datos sintéticos
```bash
python synthetic_data/src/generator.py
```

**Salida:**
```
synthetic_data/output/clientes_yyyymmdd_hhmm_seednn.json
synthetic_data/data/tdm_synthetic_data.db
``` 

### 2. Inyección de fallas
```bash
python synthetic_data/src/injector.py
```

**Salida:**
```
synthetic_data/output/clientes_yyyymmdd_hhmm_seednn.json
synthetic_data/output/report_injection.json
``` 

### 3. Validación de datos
```bash
python synthetic_data/src/validator.py
```

**Salida:**
```
synthetic_data/output/errors_yyyymmdd_hhmm_seednn.csv
synthetic_data/output/errors_yyyymmdd_hhmm_seednn.json
synthetic_data/output/report_yyyymmdd_hhmm_seednn.json
``` 

---

## Enfoque técnico

### Generación de datos sintéticos
- Uso de librería Faker
- Generación reproducible mediante seed
- Cumplimiento del contrato de datos:
  - customer_id único
  - edad entre 18–90 años
  - email coherente
  - teléfono válido
  - fechas consistentes

---

### Inyección de fallas
Se implementa inyección controlada basada en:

```
total_errores = total_registros * error_rate
```

Tipos de errores:

- schema
  - formato inválido (email, fechas, teléfono, cédula)
- domain
  - valores fuera del catálogo (estado_cliente)
- dup
  - duplicados en customer_id
- business
  - violaciones de reglas:
  - edad < 18
  - cliente inactivo con fecha reciente

Uso de seed para errores reproducibles

---

### Validación
El validador:

- Analiza cada registro
- Detecta errores generados
- Clasifica errores por tipo:
  - schema
  - domain
  - dup
  - business
- Genera métricas de calidad

---

### Métricas de calidad
- total_registros
- reglas_evaluadas
- errores_totales
- errores_por_tipo
- errores_por_regla
- %cumplimiento
- muestras_errores

Cálculo de cumplimiento:
```
%cumplimiento = (registros_sin_error / total_registros) * 100
```