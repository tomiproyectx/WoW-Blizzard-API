# 🏆 World of Warcraft E2E Pipeline
 
**Batch ELT Pipeline using WoW Blizzard APIs**
 
🇪🇸 [Versión en Español](https://github.com/tomiproyectx/WoW-Blizzard-API/blob/main/README_SPANISH.md)
 
[![Python](https://img.shields.io/badge/Python_3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow_2.x-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-1D63ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Redshift](https://img.shields.io/badge/AWS_Redshift-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/redshift/)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-000000?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)
[![Parquet](https://img.shields.io/badge/Parquet-50C878?style=for-the-badge&logo=apache&logoColor=white)](https://parquet.apache.org/)
 
---
 
## 📖 Overview
 
WoW PvP Analytics Pipeline is a production-style ELT pipeline that ingests live player data from the Blizzard API and delivers it as an analytics-ready star schema in AWS Redshift. Built as a personal project to apply real data engineering practices — orchestration, layered data modeling, testing, and CI/CD — to a domain I'm genuinely interested in. The pipeline runs daily, tracking PvP leaderboard rankings and character progression across competitive brackets (2v2 and 3v3).
 
---
 
## 🧠 Design Decisions
 
**DuckDB as local staging layer**
Rather than loading raw data directly into Redshift, DuckDB serves as a lightweight local database for raw and curated transformations. This keeps cloud costs low during development, allows fast local iteration, and decouples transformation logic from the warehouse.
 
**SCD2 on `dim_character`**
Character stats in WoW change over time — gear, rating, faction. A Type 2 Slowly Changing Dimension preserves the full history of each character's state at every snapshot, enabling accurate point-in-time analysis rather than overwriting past records.
 
**Star schema in Redshift**
The final model follows a classic star schema (fact + dimensions) to make the data intuitive for analytical queries — easy to filter by bracket, season, or character without complex joins.
 
**Medallion architecture (Landing → RAW → CUR)**
Each layer has a clear contract: Landing holds immutable raw Parquet files, RAW is a direct load with no transformations, CUR is typed and cleaned. This makes debugging straightforward — if something breaks, you know exactly which layer to inspect.
 
**Concurrent character profile extraction**
The Blizzard API requires a separate call per character. Using `ThreadPoolExecutor` reduces extraction time significantly compared to sequential requests, while staying within API rate limits.
 
---
 
## 📐 1. General Architecture
 
[![DFD – WoW PvP Data Pipeline](https://github.com/tomiproyectx/WoW-Blizzard-API/raw/main/docs/DFD%20-%20WoW%20PVP%20Pipeline.drawio.svg)](https://raw.githubusercontent.com/tomiproyectx/WoW-Blizzard-API/main/docs/DFD%20-%20WoW%20PVP%20Pipeline.png)
 
🔍 **Interactive diagram (zoom & pan)**  
[Open full diagram](https://app.diagrams.net/?title=DFD%20-%20WoW%20PVP%20Pipeline&dark=1#Uhttps%3A%2F%2Fdrive.google.com%2Fuc%3Fid%3D1EvyHY1401TK8Rg3L7pjWymF4CVcQ0qGY%26export%3Ddownload)
 
---
 
## 🌐 2. APIs Used
 
### • **PvP Season Index**
 
`/data/wow/pvp-season/index`  
Retrieves the active season (`current_season.id`).
 
### • **PvP Leaderboards (2v2 / 3v3)**
 
`/data/wow/pvp-season/{season}/pvp-leaderboard/{bracket}`  
Ranking and PvP statistics.
 
### • **Character Profile Summary**
 
`/profile/wow/character/{realmSlug}/{characterName}`  
Full character information.
 
Endpoints are defined in `src/tp2025/blizzard_api/endpoints.py`.
 
---
 
## 🔄 3. Pipeline in Detail
 
### **3.1 Leaderboard Extraction → Landing**
 
Generates daily Parquet files:  
`pvp_leaderboard_s{season}_{bracket}_{YYYYMMDD}.parquet`
 
---
 
### **3.2 RAW Leaderboard (DuckDB)**
 
Direct insert into `raw_pvp_leaderboard`.
 
---
 
### **3.3 CUR Leaderboard**
 
Typed transformation → `cur_pvp_leaderboard`
 
---
 
### **3.4 Top Character Selection**
 
Ranking by bracket and deduplication by `char_id`.  
Total limit: **500 characters**
 
---
 
### **3.5 Character Profile Extraction**
 
Concurrent requests (ThreadPoolExecutor) → Parquet:  
`ch_profile_{YYYYMMDD}.parquet`
 
---
 
### **3.6 RAW Character Info**
 
Load into `raw_chinfo`.
 
---
 
### **3.7 CUR Character Info**
 
Transformation → `cur_chinfo`.
 
---
 
### **3.8 Load into Redshift (Star Schema)**
 
**Dimensions:**
- `dim_season`
- `dim_bracket`
- `dim_character_scd2` (**true daily SCD2**)
 
**Fact table:**
- `fact_pvp_leaderboard_snapshot`
 
---
 
## 🪬 4. Airflow DAG
 
Execution order:
 
```
set_blizzard_env_vars
→ extract_leaderboard_to_landing
→ load_leaderboard_raw_to_db
→ build_leaderboard_cur
→ extract_chinfo_to_landing
→ load_chinfo_raw_to_db
→ build_chinfo_cur
→ load_redshift_model
```
 
The DAG is designed to run daily at 06:00 (`0 6 * * *`).
 
> **Note:** `schedule_interval=None` is set in the repository to facilitate manual testing. To enable daily execution, replace it with `schedule_interval="0 6 * * *"`.
 
---
 
## 🚀 5. How to Run the Project
 
### **5.1 Prerequisites**
 
Install:
- Docker + Docker Compose
- Python 3.10 (for running tests locally)
- git
 
Clone the repository:
 
```bash
git clone https://github.com/tomiproyectx/WoW-Blizzard-API.git
cd WoW-Blizzard-API
```
 
---
 
### 5.2 Configure Credentials
 
Create the `.env` file from the provided template:
 
```bash
make env
```
 
Fill in the following variables:
 
```
BLIZZARD_CLIENT_ID=your_client_id
BLIZZARD_CLIENT_SECRET=your_client_secret
BLIZZARD_REGION=us
REDSHIFT_URI=postgresql://user:pass@host:5439/db
REDSHIFT_SCHEMA=your_schema
```
 
**Blizzard credentials:** Register a free developer application at [develop.battle.net](https://develop.battle.net) to obtain your `client_id` and `client_secret`.
 
**Redshift:** You will need access to an AWS Redshift cluster. The `.env.example` file in the repository contains the full variable skeleton.
 
---
 
### 5.3 Build Image
 
```bash
make build
```
 
---
 
### 5.4 Initialize Airflow
 
```bash
make init
```
 
Creates:
- Metadata database
- Admin user
- Blizzard variables
 
---
 
### 5.5 Start Airflow
 
```bash
make up
```
 
Airflow UI: 👉 http://localhost:8080  
User: `airflow`  
Password: `airflow`
 
---
 
### 5.6 Run the DAG
 
In the Airflow UI:
- Enable the DAG
- Trigger it manually
 
Outputs:
- Parquet files → `data/landing/`
- DuckDB database → `data/localdb/wow_data.db`
 
---
 
## 6. Testing
 
Folder: `tests/`
 
Includes tests for:
- Authentication
- Leaderboard transformations
- Character info transformations
 
Run:
 
```bash
make test
```
 
GitHub Actions runs tests automatically on every pull request.
 
---
 
## 7. Preliminary Considerations (Docker & Permissions)
 
### 7.1 sudo usage depending on Docker configuration
 
If Docker requires elevated privileges:
 
```bash
sudo make build
sudo make up
sudo docker compose ps
```
 
If your user belongs to the `docker` group, this is not required.
 
---
 
### 7.2 Required data folders
 
```
data/landing/   → Parquet files
data/localdb/   → DuckDB database
```
 
Create them with:
 
```bash
mkdir -p data/landing
mkdir -p data/localdb
chmod -R 755 data/
```
 
---
 
## 8. Repository Structure
 
| Path | Description |
|------|-------------|
| `dags/wow_pvp_full_pipeline_dag.py` | Daily DAG orchestrating the full pipeline: Blizzard API → DuckDB (raw/cur) → Redshift |
| `src/tp2025/blizzard_api/auth_client.py` | Blizzard authentication (Client Credentials Flow) |
| `src/tp2025/blizzard_api/endpoints.py` | API URL construction (season, leaderboard, profile) |
| `src/tp2025/jobs/extract_leaderboard_to_landing.py` | Extracts PvP leaderboards to Parquet (landing) |
| `src/tp2025/jobs/load_leaderboard_raw_to_db.py` | Loads leaderboards into RAW (DuckDB) |
| `src/tp2025/jobs/build_leaderboard_cur.py` | Builds CUR leaderboard table |
| `src/tp2025/jobs/extract_chinfo_to_landing.py` | Selects top characters and extracts profiles to Parquet |
| `src/tp2025/jobs/load_chinfo_raw_to_db.py` | Loads character info into RAW |
| `src/tp2025/jobs/build_chinfo_cur.py` | Builds CUR character table |
| `src/tp2025/jobs/load_warehouse_redshift.py` | Reads CUR (DuckDB) and loads the star schema into Redshift |
| `src/tp2025/transforms/transform_leaderboard.py` | Casting and modeling logic for `cur_pvp_leaderboard` |
| `src/tp2025/transforms/transform_chinfo.py` | Casting and modeling logic for `cur_chinfo` |
| `src/tp2025/warehouse/connect_redshift.py` | Redshift connection and search_path |
| `src/tp2025/warehouse/redshift_model.py` | DDL and bulk loads (character SCD2 and fact snapshot) |
| `src/tp2025/io/load_localdb.py` | Helper for DuckDB connection and local SQL execution |
| `src/tp2025/services/character_selection.py` | Selection of unique top characters from CUR |
| `src/tp2025/services/ch_profile_client.py` | Concurrent requests to the character profile endpoint |
| `docker-compose.yml` | Orchestrates Postgres (metadata) and Airflow webserver/scheduler |
| `Dockerfile` | Custom Airflow image with the project installed via uv |
| `Makefile` | Shortcuts: `make env`, `make build`, `make init`, `make up`, `make down`, `make test` |
| `tests/` | Unit tests for authentication and transformations |
