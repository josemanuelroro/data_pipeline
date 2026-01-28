# 🚀 End-to-End Self-Hosted Data Lakehouse

> ⚠️ **Work in Progress:** This project is currently in **Beta**. The architecture is stable, but new scrapers and optimizations are being added weekly.

![Status](https://img.shields.io/badge/Status-Work_in_Progress-yellow)
![Docker](https://img.shields.io/badge/Containerized-Docker-blue)
![Open Source](https://img.shields.io/badge/100%25-Open%20Source-green)

## 📋 Project Overview
This project implements a comprehensive **Data Lakehouse** platform deployed entirely **Self-Hosted** using a 100% Open Source stack.

<p align="center">
  <img src="Architecture.svg" alt="Architecture Diagram" width="100%">
</p>
<br>

The system is designed to handle the full data lifecycle: from automated web scraping and ingestion to distributed processing and BI visualization. It prioritizes data integrity, scalability, and disaster recovery without relying on proprietary cloud licensing.

### 🎯 Key Value Propositions
* **Zero-Touch Automation:** Fully automated pipeline from ingestion to dashboard updates.
* **Medallion Architecture:** Strict implementation of Bronze, Silver, and Gold layers to ensure data quality.
* **Observability:** Real-time mobile alerts (Telegram) for workflow status and security events.
* **Resilience:** Automated Disaster Recovery strategies with off-site cloud backups.

---

## 🏗 Technical Architecture

The infrastructure is orchestrated via **Docker Compose**, enabling a modular, portable, and reproducible environment ("Infrastructure as Code").

### Tech Stack

| Domain | Tools | Description |
| :--- | :--- | :--- |
| **Ingestion** | 🐍 Python, Scrapers | Custom web scrapers for raw data extraction (`.csv`). |
| **Storage** | 🗄️ MinIO (S3) | Object storage Data Lake (S3 compatible). |
| **Compute / ETL** | ⚡ Apache Spark | Distributed processing, cleaning, and transformation. |
| **Query Engine** | 🚀 Trino + Hive | High-performance federated SQL engine over the Lake. |
| **Orchestration** | 🌬️ Apache Airflow | Workflow management and DAG scheduling. |
| **Visualization** | 📊 Metabase | Business Intelligence (BI) dashboards. |
| **DevOps & CI/CD** | ⚙️ Jenkins, Docker | Continuous Deployment triggered by repository changes. |
| **Infra & Security** | 🛡️ Nginx, Telegram | Reverse proxy, traffic logging, and security alerting. |

---

## 🔄 Data Pipeline (Medallion Architecture)

The data flow follows the **Bronze-Silver-Gold** paradigm to maintain a clean Lakehouse:

1.  **Ingestion (Raw):** Scrapers collect data and generate raw "dirty" `.csv` files.
2.  **Bronze Layer (Landing):** **Spark** performs minimal sanitization (critical null removal) and stores raw data in MinIO.
3.  **Silver Layer (Refined):** Heavy processing. Data is cleaned, typed, and converted into **Parquet** format (partitioned) for optimized columnar storage.
4.  **Gold Layer (Aggregated):** Business-level aggregations and views are generated for consumption.
5.  **Serving:** **Trino** queries the Parquet files (via Hive/MySQL metadata) to feed **Metabase** dashboards.

---

## 🛡️ Security & Disaster Recovery

Robustness is a core pillar of this architecture:

* **Off-site Backup:** A dedicated container running **Rclone** syncs raw `.csv` data to **Google Drive** weekly. In a catastrophic server failure event, the entire Lakehouse (Silver/Gold) can be rebuilt by reprocessing these raw backups.
* **Real-time Monitoring:** An integrated **Telegram Bot** provides instant notifications for:
    * Airflow DAG successes or failures.
    * Suspicious server access attempts (IP monitoring).
* **Reverse Proxy:** All web services are secured behind **Nginx**.

---

## 🚀 Deployment & DevOps

### CI/CD Workflow
1.  **Jenkins** continuously monitors the GitHub repository.
2.  Upon detecting a *push*, it pulls the latest changes.
3.  It automatically redeploys the updated containers to the production environment.

