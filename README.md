# Healthcare Operations & Patient Flow Analytics Platform

A production-grade mini data platform for healthcare analytics using modern data engineering tools.

## Architecture

- **Bronze Layer**: Raw CSV files in MinIO
- **Silver Layer**: Cleaned, validated datasets in MinIO
- **Gold Layer**: Analytics warehouse in PostgreSQL
- **Orchestration**: Apache Airflow
- **Visualization**: Metabase

## Tech Stack

- PostgreSQL 16-alpine
- Apache Airflow 2.11.1
- MinIO (S3-compatible storage)
- Metabase
- Docker Compose
- Python 3.14

## Quick Start

1. Copy environment template:
   ```bash
   cp .env.example .env
