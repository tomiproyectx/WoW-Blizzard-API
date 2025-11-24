<div align="center">

# 🏆 WoW PvP Leaderboard ETL Pipeline  
**ETL/ELT end-to-end usando Blizzard APIs, Airflow, DuckDB, Parquet y AWS Redshift**

<br>

![Python](https://img.shields.io/badge/Python_3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow_2.x-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-1D63ED?style=for-the-badge&logo=docker&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)
![Redshift](https://img.shields.io/badge/AWS_Redshift-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-000000?style=for-the-badge&logo=githubactions&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-50C878?style=for-the-badge&logo=apache&logoColor=white)

<br>

⚔️ Datos PvP | 🧙 Transformaciones | 🐤 DuckDB | 🚀 Redshift | ♻️ Airflow  
**Pipeline diario de datos**

</div>

---

# 📐 1. Arquitectura General



            ┌──────────────────────┐
            │    Blizzard API      │
            │  (Season / PvP /     │
            │   Character Info)    │
            └──────────┬───────────┘
                       │
                       ▼
               Landing (Parquet)
                       │
                       ▼
                RAW Layer (DuckDB)
                       │
                       ▼
             CUR Layer (DuckDB – Typed)
                       │
                       ▼
          AWS Redshift (Star Schema + SCD2)
                       │
                       ▼
           Airflow DAG (Docker Compose)


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

Frecuencia: **06:00 AM diario**  
Airflow carga automáticamente credenciales Blizzard vía Variables.

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
