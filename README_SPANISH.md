<div align="center">

# 🏆 World of Warcraft E2E Pipeline  
**Pipeline Batch ELT usando WoW Blizzard APIs**

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

# 📐 1. Arquitectura General

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

</div>

---

# 🌐 2. APIs Utilizadas

### • **PvP Season Index**  
`/data/wow/pvp-season/index`  
Obtiene la temporada vigente (`current_season.id`).

### • **PvP Leaderboards (2v2 / 3v3)**  
`/data/wow/pvp-season/{season}/pvp-leaderboard/{bracket}`  
Ranking + estadísticas PvP.

### • **Character Profile Summary**  
`/profile/wow/character/{realmSlug}/{characterName}`  
Información completa del personaje.

Los endpoints viven en:  
`src/tp2025/blizzard_api/endpoints.py`

---

# 🔄 3. Pipeline en Detalle

## **3.1 Extracción Leaderboard → Landing**
Genera Parquets diarios:
pvp_leaderboard_s{season}{bracket}{YYYYMMDD}.parquet


---

## **3.2 RAW Leaderboard (DuckDB)**
Insert directo a:
raw_pvp_leaderboard


---

## **3.3 CUR Leaderboard**
Transformación tipada → `cur_pvp_leaderboard`

---

## **3.4 Selección de Personajes Top**
Ranking por bracket y deduplicación por `char_id`  
Límite total: **500 personajes**

---

## **3.5 Extracción Character Profiles**
Request concurrente (ThreadPoolExecutor) → Parquet:
ch_profile_{YYYYMMDD}.parquet

---

## **3.6 RAW Character Info**
Carga en `raw_chinfo`.

---

## **3.7 CUR Character Info**
Transformación → `cur_chinfo`.

---

## **3.8 Carga en Redshift (Modelo Estrella)**

### Dimensiones:
- `dim_season`
- `dim_bracket`
- `dim_character_scd2` (**SCD2 diario real**)

### Tabla de hechos:
- `fact_pvp_leaderboard_snapshot`

---

# 🪬 4. DAG de Airflow

Orden real:

set_blizzard_env_vars
→ extract_leaderboard_to_landing
→ load_leaderboard_raw_to_db
→ build_leaderboard_cur
→ extract_chinfo_to_landing
→ load_chinfo_raw_to_db
→ build_chinfo_cur
→ load_redshift_model

El DAG está diseñado para correr diariamente a las 06:00 (0 6 * * *).

**ACLARACIÓN**: En este repo se deja el schedule_interval=None para facilitar pruebas manuales.
Para activar la ejecución diaria, basta con reemplazar schedule_interval=None por schedule_interval="0 6 * * *".

---

# 🚀 5. Cómo Ejecutar el Proyecto

## **5.1 Requisitos previos**
Instalar:
- Docker + Docker Compose  
- Python 3.10 (solo para tests)  
- git  

Clonar:

```bash
git clone https://github.com/tomiproyectx/WoW-Blizzard-API.git
cd WoW-Blizzard-API
```
## **5.2 Configurar credenciales**

Crear .env:
```bash
make env
```
Completar:

***BLIZZARD_CLIENT_ID***=xxxx

***BLIZZARD_CLIENT_SECRET***=xxxx

***BLIZZARD_REGION***=us

***REDSHIFT_URI***=postgresql://user:pass@host:5439/db

***REDSHIFT_SCHEMA***=2025_usuario_schema

> Las credenciales reales de Blizzard y Redshift se entregan por privado (mail / Slack),
> el archivo `.env` del repo solo contiene el esqueleto de variables.

## **5.3 Construir imagen**
```bash
make build
```
## **5.4 Inicializar Airflow**
```bash
make init
```
Crea:

metadata DB

usuario admin

variables Blizzard

## **5.5 Levantar Airflow**
```bash
make up
```
UI:
👉 http://localhost:8080
User: airflow
Password: airflow

## **5.6 Ejecutar DAG**

En Airflow:

Activar DAG

Trigger manual

Genera:

Parquets → data/landing/

DuckDB → data/localdb/wow_data.db

# 6. Testing

Carpeta: tests/
Incluye tests para:

autenticación

transformaciones leaderboard

transformaciones chinfo

Ejecutar:
```bash
make test
```
GitHub Actions ejecuta los tests en cada PR.

# 7. Consideraciones Previas (Docker & Permisos)

## **7.1 Uso de sudo según configuración Docker**

Si Docker requiere privilegios:
```bash
sudo make build
sudo make up
sudo docker compose ps
```

Si el usuario pertenece al grupo docker, no es necesario.

## **7.2 Carpeta data/ requerida**

data/landing/   → Parquets
data/localdb/   → Base DuckDB
``` bash
mkdir -p data/landing
mkdir -p data/localdb
chmod -R 755 data/
```

# 8. Estructura del repositorio (alto nivel)

- `dags/wow_pvp_full_pipeline_dag.py`  
  DAG diario que orquesta todo el pipeline:
  Blizzard API → DuckDB (raw/cur) → Redshift.

- `src/tp2025/blizzard_api/`  
  - `auth_client.py`: autenticación contra Blizzard (Client Credentials Flow).
  - `endpoints.py`: construcción de URLs de las APIs (season, leaderboard, profile).

- `src/tp2025/jobs/`  
  - `extract_leaderboard_to_landing.py`: extrae PvP leaderboards a Parquet (landing).
  - `load_leaderboard_raw_to_db.py`: carga leaderboards a tabla RAW (DuckDB).
  - `build_leaderboard_cur.py`: genera tabla CUR de leaderboard.
  - `extract_chinfo_to_landing.py`: selecciona top chars y extrae profiles a Parquet.
  - `load_chinfo_raw_to_db.py`: carga info de personajes a RAW.
  - `build_chinfo_cur.py`: genera tabla CUR de personajes.
  - `load_warehouse_redshift.py`: lee CUR (DuckDB) y carga modelo estrella en Redshift.

- `src/tp2025/transforms/`  
  - `transform_leaderboard.py`: lógica de casteo y modelado de `cur_pvp_leaderboard`.
  - `transform_chinfo.py`: lógica de casteo y modelado de `cur_chinfo`.

- `src/tp2025/warehouse/`  
  - `connect_redshift.py`: conexión y `search_path` a Redshift.
  - `redshift_model.py`: DDL + cargas bulk (SCD2 de personajes y fact snapshot).

- `src/tp2025/io/load_localdb.py`  
  Helper para conexión a DuckDB y ejecución de SQL local.

- `src/tp2025/services/`  
  - `character_selection.py`: selección de top personajes únicos desde CUR.
  - `ch_profile_client.py`: requests concurrentes al endpoint de perfil de personaje.

- `docker-compose.yml`  
  Orquesta Postgres (metadata) + Airflow webserver/scheduler.

- `Dockerfile`  
  Imagen custom de Airflow con el proyecto instalado vía `uv`.

- `Makefile`  
  Atajos: `make env`, `make build`, `make init`, `make up`, `make down`, `make test`.

- `tests/`  
  Tests unitarios para autenticación y transformaciones (leaderboard + chinfo).
