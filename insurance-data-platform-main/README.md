# Insurance Data Platform

End-to-end insurance data engineering platform on Databricks using the **Medallion Architecture** (Bronze → Silver → Gold). Processes ~100-200GB of insurance data across 5 lines of business, delivering a normalized star schema and a policy-centric data mart.

## Lines of Business

| LOB | Code | Data Formats |
|-----|------|-------------|
| Property | PROP | CSV, JSON, Parquet |
| Workers' Compensation | WC | CSV, JSON, XML |
| Auto | AUTO | CSV, JSON, Parquet |
| General Liability | GL | CSV, JSON, XML |
| Umbrella | UMB | CSV, JSON, Parquet |

## Architecture

```
Raw Data Sources (FEMA, CAS, Kaggle, Synthetic)
        │
        ▼
┌─────────────────────────────┐
│   BRONZE (Raw Landing)      │  Schema-on-read, all formats
│   insurance_catalog.bronze  │  Metadata: _ingestion_timestamp,
│                             │  _source_file, _batch_id
└─────────┬───────────────────┘
          │
          ▼
┌─────────────────────────────┐
│   SILVER (Cleansed)         │  Type casting, dedup, DQ flags
│   insurance_catalog.silver  │  snake_case standardization
│                             │  _is_valid, _dq_issues columns
└─────────┬───────────────────┘
          │
          ▼
┌─────────────────────────────┐
│   GOLD (Business-Ready)     │  3NF Normalized Star Schema
│   insurance_catalog.gold    │  8 Dimensions + 3 Facts
│                             │  + mart_policy_360
└─────────────────────────────┘
```

## Gold Layer Schema

### Dimensions
- `dim_date` — Date calendar (1970-2035)
- `dim_line_of_business` — 5 LOBs with NAIC codes
- `dim_insured` — Policyholders and businesses
- `dim_location` — Addresses with lat/long, flood zones
- `dim_agent` — Agents and agencies
- `dim_coverage` — Coverage types per LOB
- `dim_policy` — Central policy dimension (FK: all above)
- `dim_claim` — Claims across all LOBs (FK: policy)

### Facts
- `fact_premium` — Written/earned/net premium per policy-coverage
- `fact_claim_transaction` — Paid/reserve/recovery per claim
- `fact_policy_transaction` — Policy lifecycle events

### Data Mart
- `mart_policy_360` — Denormalized wide table, one row per policy_id, complete 360-degree view including LOB-specific attributes, premium/claims aggregates, and loss ratio

## Data Sources

### Public Data (~15-25GB)
- **FEMA NFIP Claims/Policies** — Property flood insurance (2.7M+ claims, 72M+ policies)
- **CAS Schedule P** — Actuarial loss triangles (Auto, WC, GL)
- **Kaggle** — Auto/Home insurance datasets

### Synthetic Data (~100-175GB)
Generated via `dbldatagen` (Databricks) or `Faker` (local), seeded for reproducibility.

## Project Structure

```
insurance-data-platform/
├── .github/workflows/databricks-ci.yml
├── config/
│   ├── pipeline_config.yaml
│   └── data_quality_rules.yaml
├── src/
│   ├── common/           # Schemas, utils, DQ framework
│   ├── ingestion/        # Download + synthetic generation
│   ├── bronze/           # Raw ingestion (CSV, JSON, XML → Delta)
│   ├── silver/           # Transforms per LOB (5 scripts)
│   ├── gold/
│   │   ├── dimensions/   # 8 dimension builders
│   │   ├── facts/        # 3 fact builders
│   │   └── mart/         # mart_policy_360
│   └── workflows/        # Databricks job definition
├── notebooks/            # Databricks notebooks (setup, ingestion, EDA, validation)
├── tests/                # Unit tests
└── requirements.txt
```

## Databricks Workflow

```
setup_catalog → generate_data → [ingest_structured, ingest_semi_structured]
  → [transform_property, transform_wc, transform_auto, transform_gl, transform_umbrella]
    → build_dimensions → build_facts → build_mart → validate_mart
```

## Quick Start

### 1. Connect GitHub to Databricks
```
Databricks Workspace → Repos → Add Repo → https://github.com/<org>/insurance-data-platform.git
```

### 2. Run Setup
```sql
-- In Databricks SQL
RUN notebooks/00_setup_catalog
```

### 3. Run Pipeline
```python
# In notebook or via workflow
from src.ingestion.generate_synthetic import SyntheticDataGenerator
gen = SyntheticDataGenerator(spark, mode="local")  # or "databricks" for full scale
gen.generate_all()
```

### 4. Query the Mart
```sql
SELECT * FROM insurance_catalog.gold.mart_policy_360
WHERE policy_id = 'AUTO-0000000001';
```

## CI/CD

GitHub Actions pipeline:
1. **Lint** — flake8, black format check
2. **Test** — pytest unit tests
3. **Deploy** — Syncs repo to Databricks and updates workflow (on push to `main`)

Required GitHub Secrets:
- `DATABRICKS_HOST` — Workspace URL
- `DATABRICKS_TOKEN` — Personal access token

## Surrogate Key Strategy

All surrogate keys use `MD5(CONCAT(natural_key_columns))` for deterministic, idempotent generation without sequence dependencies.

## Technologies

- **Databricks** — Unity Catalog, Delta Lake, Workflows
- **PySpark** — All transformations
- **dbldatagen** — Synthetic data generation at scale
- **GitHub Actions** — CI/CD pipeline
