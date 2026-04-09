# Olist E-Commerce Pipeline & Delivery Forecasting

An end-to-end data engineering and machine learning project built on Databricks, implementing the medallion architecture to transform raw e-commerce data into ML-ready features for delivery time and freight value prediction.

---

## Project Overview

This project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) to build a production-style data pipeline and predictive models that estimate:

1. **Delivery Time**: How many days until an order reaches the customer
2. **Freight Value**: Expected shipping cost based on product and location

The end result is a **Streamlit application** where users can input order parameters and receive real-time predictions.

---

## Medallion Architecture

<img width="2788" height="673" alt="olist_project_diagram" src="https://github.com/user-attachments/assets/151d7869-446d-40a2-8913-ac05d0aa81c2" />


### Architecture Details

| Layer | Purpose | Tables |
|-------|---------|--------|
| **Bronze** | Raw ingestion, no transformations | 9 tables (as-is from source) |
| **Silver** | Cleaned, typed, enriched | 11 tables (joins, geo-enrichment) |
| **Gold** | ML-ready feature stores | 3 datasets (one per model + sentiment) |
| **Application** | Aggregated metrics | Seller Performance Dashboard |

---

## Data Flow

### Bronze → Silver Transformations

```
Bronze                          Silver
──────                          ──────
customers         ──►   customers_cleaned ──► customers_with_location
sellers           ──►   sellers_cleaned   ──► sellers_with_location
geolocation       ──►   geolocation_cleaned
                                    │
                                    ▼
                        customer_seller_proximity

orders            ──►   orders_cleaned    ──┐
order_items       ──►   order_items_cleaned ├──► order_details
order_payments    ──►   order_payments_cleaned ─┘

order_reviews     ──►   order_reviews_cleaned

products          ──►   product_cleaned   ──┬──► product_details
product_category  ──►   (passthrough)     ──┘
```

### Silver → Gold Feature Engineering

| Gold Table | Source Tables | Purpose |
|------------|---------------|---------|
| `delivery_date_predictor_dataset` | customer_seller_proximity, order_details | Features for delivery time model |
| `freight_value_predictor_dataset` | product_details, order_details | Features for freight cost model |
| `reviews_sentiment_dataset` | order_reviews_cleaned | Sentiment analysis + seller metrics |

---

## ML Models

### 1. Delivery Time Predictor

Predicts estimated delivery days based on:
- Distance between customer and seller (haversine)
- Seller historical performance
- Order timing (day of week, month)
- Product weight/dimensions

### 2. Freight Value Predictor

Predicts shipping cost based on:
- Product dimensions and weight
- Volumetric weight (L × W × H / 6000)
- Seller location
- Customer location

### 3. Review Sentiment Analysis

Extracts delay-related complaints from Portuguese reviews to create seller-level complaint rates (used as feature in delivery model, avoiding data leakage).

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Data Platform | Databricks (Unity Catalog) |
| Storage Format | Delta Lake |
| Processing | PySpark |
| ML Training | LightGBM / scikit-learn |
| Experiment Tracking | MLflow |
| Model Explainability | SHAP |
| Application | Streamlit |
| Version Control | Git / GitHub |

---

## Project Structure

```
olist_pipeline_and_forecasting/
│
├── src/
│   ├── utils/
│   │   ├── __init__.py
│   │   └── logger.py              # Pipeline logging to Delta table
│   │
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingest.py              # Raw CSV → Bronze Delta tables
│   │
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── transform.py           # Cleaning & type casting functions
│   │   ├── enrich.py              # Geo enrichment, distance calc
│   │   └── build_silver.py        # Silver table orchestration
│   │
│   └── gold/
│       ├── __init__.py
│       └── features.py            # Feature engineering for ML
│
├── notebooks/
│   ├── eda/                       # Exploratory notebooks per table
│   │   ├── eda_customers.ipynb
│   │   ├── eda_orders.ipynb
│   │   └── ...
│   │
│   ├── BronzeLayer.ipynb          # Bronze ingestion runner
│   ├── SilverLayer.ipynb          # Silver transformation runner
│   └── GoldLayer.ipynb            # Feature engineering runner
│
├── jobs/
│   └── daily_pipeline.py          # Databricks Workflow orchestration
│
├── streamlit_app/
│   ├── app.py                     # Main Streamlit application
│   └── model/                     # Serialized models + SHAP explainer
│
├── tests/
│   ├── test_bronze.py             # Schema & null checks
│   ├── test_silver.py             # Join validation tests
│   ├── test_gold.py               # Feature validity tests
│   └── test_utils.py              # Logger tests
│
└── README.md
```

---

## Database Schema

All tables live in the `workspace` catalog:

| Schema | Purpose |
|--------|---------|
| `workspace.olist_bronze` | Raw ingested data |
| `workspace.olist_silver` | Cleaned & joined data |
| `workspace.olist_gold` | ML feature stores |
| `workspace.olist_metadata` | Pipeline logs |

---

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to create schemas and tables
- Python 3.10+

### Setup

1. Clone the repository to Databricks Repos:
   ```
   Repos → Add Repo → https://github.com/yourusername/olist_pipeline_and_forecasting
   ```

2. Upload raw Olist CSV files to a Volume:
   ```
   /Volumes/workspace/olist/raw_data/
   ```

3. Run the Bronze layer notebook to ingest data:
   ```
   notebooks/BronzeLayer.ipynb
   ```

4. Run Silver and Gold layer notebooks in sequence.

5. Launch the Streamlit app:
   ```bash
   cd streamlit_app
   streamlit run app.py
   ```

---

## Pipeline Monitoring

All pipeline runs are logged to `workspace.olist_metadata.pipeline_log`:

```sql
SELECT * FROM workspace.olist_metadata.pipeline_log
ORDER BY logged_at DESC
LIMIT 20;
```

| Column | Description |
|--------|-------------|
| layer | bronze / silver / gold |
| table_name | Target table |
| rows_in | Input row count |
| rows_out | Output row count |
| status | SUCCESS / FAILED |
| duration_sec | Execution time |

---

## Key Design Decisions

### Data Leakage Prevention
Review sentiment is aggregated at the **seller level** (rolling 90-day complaint rate) rather than order level, preventing leakage where a review about Order X would predict delivery for Order X.

### Schema Inference Disabled
Bronze layer reads all columns as strings (`inferSchema=False`) to preserve raw data exactly as received. Type casting happens in Silver layer.

### Idempotent Writes
All table writes use `mode("overwrite")` with `overwriteSchema=True` for reproducible pipeline runs.

---

## Results

| Model | Metric | Value |
|-------|--------|-------|
| Delivery Time | MAE | X days |
| Freight Value | MAE | R$ X |

*(To be updated after model training)*

---

## Future Improvements

- [ ] Add real-time streaming ingestion
- [ ] Implement model serving via MLflow Model Serving
- [ ] Add automated retraining with drift detection
- [ ] Deploy infrastructure with Terraform

---

## Author

**Apoorva**  


---

## License

This project uses the [Olist Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) released under CC BY-NC-SA 4.0.
