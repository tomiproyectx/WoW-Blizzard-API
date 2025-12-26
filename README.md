<div align="center">

# 🏆 World of Warcraft E2E Pipeline  
**Batch ELT Pipeline using WoW Blizzard APIs**

:es: [Versión en Español](README_SPANISH.md)

<br>

![Python](https://img.shields.io/badge/Python_3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow_2.x-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-1D63ED?style=for-the-badge&logo=docker&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![Redshift](https://img.shields.io/badge/AWS_Redshift-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-000000?style=for-the-badge&logo=githubactions&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-50C878?style=for-the-badge&logo=apache&logoColor=white)

<br>

</div>

---

# 📐 1. General Architecture

<div align="center">
  <a href="https://raw.githubusercontent.com/tomiproyectx/WoW-Blizzard-API/main/docs/DFD%20-%20WoW%20PVP%20Pipeline.png">
    <img
      src="./docs/DFD%20-%20WoW%20PVP%20Pipeline.drawio.svg"
      alt="DFD – WoW PvP Data Pipeline"
      width="1200"
    />
  </a>
</div>

🔍 **Interactive diagram (zoom & pan)**  
[Open full diagram](https://app.diagrams.net/?title=DFD%20-%20WoW%20PVP%20Pipeline&dark=1#Uhttps%3A%2F%2Fdrive.google.com%2Fuc%3Fid%3D1EvyHY1401TK8Rg3L7pjWymF4CVcQ0qGY%26export%3Ddownload)

---

# 🌐 2. APIs Used

### • **PvP Season Index**  
`/data/wow/pvp-season/index`  
Retrieves the active season (`current_season.id`).

### • **PvP Leaderboards (2v2 / 3v3)**  
`/data/wow/pvp-season/{season}/pvp-leaderboard/{bracket}`  
Ranking and PvP statistics.

### • **Character Profile Summary**  
`/profile/wow/character/{realmSlug}/{characterName}`  
Full character information.

Endpoints are defined in:  
`src/tp2025/blizzard_api/endpoints.py`

---

# 🔄 3. Pipeline in Detail

## **3.1 Leaderboard Extraction → Landing**
Generates daily Parquet files:  
`pvp_leaderboard_s{season}{bracket}{YYYYMMDD}.parquet`

---

## **3.2 RAW Leaderboard (DuckDB)**
Direct insert into:  
`raw_pvp_leaderboard`

---

## **3.3 CUR Leaderboard**
Typed transformation → `cur_pvp_leaderboard`

---

## **3.4 Top Character Selection**
Ranking by bracket and deduplication by `char_id`  
Total limit: **500 characters**

---

## **3.5 Character Profile Extraction**
Concurrent requests (ThreadPoolExecutor) → Parquet:  
`ch_profile_{YYYYMMDD}.parquet`

---

## **3.6 RAW Character Info**
Load into `raw_chinfo`.

---

## **3.7 CUR Character Info**
Transformation → `cur_chinfo`.

---

## **3.8 Load into Redshift (Star Schema)**

### Dimensions:
- `dim_season`
- `dim_bracket`
- `dim_character_scd2` (**true daily SCD2**)

### Fact table:
- `fact_pvp_leaderboard_snapshot`

---

# 🪬 4. Airflow DAG

Execution order:

→ set_blizzard_env_vars          

→ extract_leaderboard_to_landing 

→ load_leaderboard_raw_to_db  

→ build_leaderboard_cur  

→ extract_chinfo_to_landing  

→ load_chinfo_raw_to_db  

→ build_chinfo_cur  

→ load_redshift_model  

The DAG is designed to run daily at 06:00 (0 6 * * *).

**NOTE**: In this repository, `schedule_interval=None` is set to facilitate manual testing.  
To enable daily execution, simply replace `schedule_interval=None` with `schedule_interval="0 6 * * *"`.

---

# 🚀 5. How to Run the Project

## **5.1 Prerequisites**

Install:

- Docker + Docker Compose  

- Python 3.10 (tests only) 

- git  

Clone the repository:

git clone https://github.com/tomiproyectx/WoW-Blizzard-API.git  

cd WoW-Blizzard-API

---

## 5.2 Configure Credentials

Create the `.env` file:

```bash
make env
```

Fill in:

BLIZZARD_CLIENT_ID=xxxx

BLIZZARD_CLIENT_SECRET=xxxx  

BLIZZARD_REGION=us 

REDSHIFT_URI=postgresql://user:pass@host:5439/db  

REDSHIFT_SCHEMA=2025_user_schema

> Real Blizzard and Redshift credentials are provided privately (email / Slack).  
> The `.env` file in the repository only contains the variable skeleton.

---

## 5.3 Build Image

```bash
make build
```

---

## 5.4 Initialize Airflow

```bash
make init
```

Creates:

- Metadata database

- Admin user

- Blizzard variables

---

## 5.5 Start Airflow

```bash
make up
```

Airflow UI:  

👉 http://localhost:8080  

User: `airflow`  

Password: `airflow`

---

## 5.6 Run the DAG

In Airflow:

- Enable the DAG

- Trigger it manually

Outputs:

- Parquet files → `data/landing/`

- DuckDB database → `data/localdb/wow_data.db`

---

# 6. Testing

Folder: `tests/`

Includes tests for:

- Authentication

- Leaderboard transformations

- Character info transformations

Run:

```bash
make test
```

GitHub Actions runs tests on every pull request.

---

# 7. Preliminary Considerations (Docker & Permissions)

## 7.1 sudo usage depending on Docker configuration

If Docker requires privileges:

```bash
sudo make build  
sudo make up  
sudo docker compose ps
```

If the user belongs to the docker group, this is not required.

---

## 7.2 Required data folders

```haskell
data/landing/   → Parquet files  
data/localdb/   → DuckDB database
```

Create them:

```bash
mkdir -p data/landing  
mkdir -p data/localdb  
chmod -R 755 data/
```

---

# 8. Repository Structure (High Level)

- `dags/wow_pvp_full_pipeline_dag.py` 

  Daily DAG orchestrating the full pipeline:  
  
  Blizzard API → DuckDB (raw/cur) → Redshift.

- `src/tp2025/blizzard_api/`

  - `auth_client.py`: Blizzard authentication (Client Credentials Flow).

  - `endpoints.py`: API URL construction (season, leaderboard, profile).

- `src/tp2025/jobs/`

  - `extract_leaderboard_to_landing.py`: Extracts PvP leaderboards to Parquet (landing).

  - `load_leaderboard_raw_to_db.py`: Loads leaderboards into RAW (DuckDB).

  - `build_leaderboard_cur.py`: Builds CUR leaderboard table.

  - `extract_chinfo_to_landing.py`: Selects top characters and extracts profiles to Parquet.

  - `load_chinfo_raw_to_db.py`: Loads character info into RAW.

  - `build_chinfo_cur.py`: Builds CUR character table.

  - `load_warehouse_redshift.py`: Reads CUR (DuckDB) and loads the star schema into Redshift.

- `src/tp2025/transforms/`

  - `transform_leaderboard.py`: Casting and modeling logic for cur_pvp_leaderboard.

  - `transform_chinfo.py`: Casting and modeling logic for cur_chinfo.

- `src/tp2025/warehouse/`

  - `connect_redshift.py`: Redshift connection and search_path.

  - `redshift_model.py`: DDL and bulk loads (character SCD2 and fact snapshot).

- `src/tp2025/io/load_localdb.py`

  Helper for DuckDB connection and local SQL execution.

- `src/tp2025/services/`

  - `character_selection.py`: Selection of unique top characters from CUR.

  - `ch_profile_client.py`: Concurrent requests to the character profile endpoint.

- `docker-compose.yml`

  Orchestrates Postgres (metadata) and Airflow webserver/scheduler.

- `Dockerfile`  
  
  Custom Airflow image with the project installed via uv.

- `Makefile`

  Shortcuts: make env, make build, make init, make up, make down, make test.

- `tests/`

  Unit tests for authentication and transformations (leaderboard and character info).
