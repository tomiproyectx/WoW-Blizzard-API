# 🏆 World of Warcraft E2E Pipeline
 
**Pipeline ELT batch usando las APIs de Blizzard**
 
🇺🇸 [English Version](https://github.com/tomiproyectx/WoW-Blizzard-API/blob/main/README.md)
 
[![Python](https://img.shields.io/badge/Python_3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Airflow](https://img.shields.io/badge/Airflow_2.x-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-1D63ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Redshift](https://img.shields.io/badge/AWS_Redshift-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/redshift/)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-000000?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)
[![Parquet](https://img.shields.io/badge/Parquet-50C878?style=for-the-badge&logo=apache&logoColor=white)](https://parquet.apache.org/)
 
---
 
## 📖 Descripción general
 
WoW PvP Analytics Pipeline es un pipeline ELT de estilo productivo que ingesta datos de jugadores en tiempo real desde la API de Blizzard y los entrega como un star schema listo para análisis en AWS Redshift. Construido como proyecto personal para aplicar prácticas reales de data engineering — orquestación, modelado de datos por capas, testing y CI/CD — en un dominio que me apasiona. El pipeline corre diariamente y registra rankings del leaderboard PvP y la progresión de personajes en los brackets competitivos (2v2 y 3v3).
 
---
 
## 🧠 Decisiones de diseño
 
**DuckDB como capa de staging local**
En lugar de cargar los datos crudos directamente en Redshift, DuckDB funciona como base de datos local liviana para las transformaciones raw y curated. Esto reduce los costos en la nube durante el desarrollo, permite iterar rápido en local y desacopla la lógica de transformación del warehouse.
 
**SCD2 en `dim_character`**
Las estadísticas de los personajes en WoW cambian con el tiempo — equipamiento, rating, facción. Una Slowly Changing Dimension de Tipo 2 preserva el historial completo del estado de cada personaje en cada snapshot, permitiendo análisis point-in-time precisos en lugar de sobreescribir registros anteriores.
 
**Star schema en Redshift**
El modelo final sigue un star schema clásico (fact + dimensions) para que las consultas analíticas sean intuitivas — fácil de filtrar por bracket, temporada o personaje sin joins complejos.
 
**Arquitectura medallion (Landing → RAW → CUR)**
Cada capa tiene un contrato claro: Landing almacena archivos Parquet inmutables, RAW es una carga directa sin transformaciones, CUR está tipada y limpia. Esto hace que el debugging sea directo — si algo falla, sabés exactamente en qué capa buscar.
 
**Extracción concurrente de perfiles de personajes**
La API de Blizzard requiere una llamada separada por personaje. Usar `ThreadPoolExecutor` reduce significativamente el tiempo de extracción comparado con requests secuenciales, manteniéndose dentro de los rate limits de la API.
 
---
 
## 📐 1. Arquitectura general
 
[![DFD – WoW PvP Data Pipeline](https://github.com/tomiproyectx/WoW-Blizzard-API/raw/main/docs/DFD%20-%20WoW%20PVP%20Pipeline.drawio.svg)](https://raw.githubusercontent.com/tomiproyectx/WoW-Blizzard-API/main/docs/DFD%20-%20WoW%20PVP%20Pipeline.png)
 
🔍 **Diagrama interactivo (zoom & pan)**  
[Abrir diagrama completo](https://app.diagrams.net/?title=DFD%20-%20WoW%20PVP%20Pipeline&dark=1#Uhttps%3A%2F%2Fdrive.google.com%2Fuc%3Fid%3D1EvyHY1401TK8Rg3L7pjWymF4CVcQ0qGY%26export%3Ddownload)
 
---
 
## 🌐 2. APIs utilizadas
 
### • **PvP Season Index**
 
`/data/wow/pvp-season/index`  
Obtiene la temporada activa (`current_season.id`).
 
### • **PvP Leaderboards (2v2 / 3v3)**
 
`/data/wow/pvp-season/{season}/pvp-leaderboard/{bracket}`  
Rankings y estadísticas PvP.
 
### • **Character Profile Summary**
 
`/profile/wow/character/{realmSlug}/{characterName}`  
Información completa del personaje.
 
Los endpoints están definidos en `src/tp2025/blizzard_api/endpoints.py`.
 
---
 
## 🔄 3. Pipeline en detalle
 
### **3.1 Extracción de Leaderboard → Landing**
 
Genera archivos Parquet diarios:  
`pvp_leaderboard_s{season}_{bracket}_{YYYYMMDD}.parquet`
 
---
 
### **3.2 RAW Leaderboard (DuckDB)**
 
Inserción directa en `raw_pvp_leaderboard`.
 
---
 
### **3.3 CUR Leaderboard**
 
Transformación tipada → `cur_pvp_leaderboard`
 
---
 
### **3.4 Selección de top personajes**
 
Ranking por bracket y deduplicación por `char_id`.  
Límite total: **500 personajes**
 
---
 
### **3.5 Extracción de perfiles de personajes**
 
Requests concurrentes (ThreadPoolExecutor) → Parquet:  
`ch_profile_{YYYYMMDD}.parquet`
 
---
 
### **3.6 RAW Character Info**
 
Carga en `raw_chinfo`.
 
---
 
### **3.7 CUR Character Info**
 
Transformación → `cur_chinfo`.
 
---
 
### **3.8 Carga en Redshift (Star Schema)**
 
**Dimensiones:**
- `dim_season`
- `dim_bracket`
- `dim_character_scd2` (**SCD2 diario real**)
 
**Tabla de hechos:**
- `fact_pvp_leaderboard_snapshot`
 
---
 
## 🪬 4. Airflow DAG
 
Orden de ejecución:
 
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
 
El DAG está configurado para correr diariamente a las 06:00 (`0 6 * * *`).
 
> **Nota:** En el repositorio se usa `schedule_interval=None` para facilitar el testing manual. Para habilitar la ejecución diaria, reemplazarlo por `schedule_interval="0 6 * * *"`.
 
---
 
## 🚀 5. Cómo ejecutar el proyecto
 
### **5.1 Prerequisitos**
 
Instalar:
- Docker + Docker Compose
- Python 3.10 (solo para correr tests localmente)
- git
 
Clonar el repositorio:
 
```bash
git clone https://github.com/tomiproyectx/WoW-Blizzard-API.git
cd WoW-Blizzard-API
```
 
---
 
### 5.2 Configurar credenciales
 
Crear el archivo `.env` desde la plantilla incluida:
 
```bash
make env
```
 
Completar las siguientes variables:
 
```
BLIZZARD_CLIENT_ID=tu_client_id
BLIZZARD_CLIENT_SECRET=tu_client_secret
BLIZZARD_REGION=us
REDSHIFT_URI=postgresql://user:pass@host:5439/db
REDSHIFT_SCHEMA=tu_schema
```
 
**Credenciales de Blizzard:** Registrá una aplicación gratuita en [develop.battle.net](https://develop.battle.net) para obtener tu `client_id` y `client_secret`.
 
**Redshift:** Necesitás acceso a un cluster de AWS Redshift. El archivo `.env.example` del repositorio contiene el esqueleto completo de variables.
 
---
 
### 5.3 Construir la imagen
 
```bash
make build
```
 
---
 
### 5.4 Inicializar Airflow
 
```bash
make init
```
 
Crea:
- Base de datos de metadatos
- Usuario administrador
- Variables de Blizzard
 
---
 
### 5.5 Levantar Airflow
 
```bash
make up
```
 
UI de Airflow: 👉 http://localhost:8080  
Usuario: `airflow`  
Contraseña: `airflow`
 
---
 
### 5.6 Ejecutar el DAG
 
En la UI de Airflow:
- Habilitar el DAG
- Triggerarlo manualmente
 
Outputs:
- Archivos Parquet → `data/landing/`
- Base de datos DuckDB → `data/localdb/wow_data.db`
 
---
 
## 6. Testing
 
Carpeta: `tests/`
 
Incluye tests para:
- Autenticación
- Transformaciones del leaderboard
- Transformaciones de info de personajes
 
Ejecutar:
 
```bash
make test
```
 
GitHub Actions corre los tests automáticamente en cada pull request.
 
---
 
## 7. Consideraciones previas (Docker & permisos)
 
### 7.1 Uso de sudo según la configuración de Docker
 
Si Docker requiere privilegios elevados:
 
```bash
sudo make build
sudo make up
sudo docker compose ps
```
 
Si tu usuario pertenece al grupo `docker`, esto no es necesario.
 
---
 
### 7.2 Carpetas de datos requeridas
 
```
data/landing/   → Archivos Parquet
data/localdb/   → Base de datos DuckDB
```
 
Crearlas con:
 
```bash
mkdir -p data/landing
mkdir -p data/localdb
chmod -R 755 data/
```
 
---
 
## 8. Estructura del repositorio
 
| Ruta | Descripción |
|------|-------------|
| `dags/wow_pvp_full_pipeline_dag.py` | DAG diario que orquesta el pipeline completo: Blizzard API → DuckDB (raw/cur) → Redshift |
| `src/tp2025/blizzard_api/auth_client.py` | Autenticación con Blizzard (Client Credentials Flow) |
| `src/tp2025/blizzard_api/endpoints.py` | Construcción de URLs de la API (temporada, leaderboard, perfil) |
| `src/tp2025/jobs/extract_leaderboard_to_landing.py` | Extrae los leaderboards PvP a Parquet (landing) |
| `src/tp2025/jobs/load_leaderboard_raw_to_db.py` | Carga los leaderboards en RAW (DuckDB) |
| `src/tp2025/jobs/build_leaderboard_cur.py` | Construye la tabla CUR del leaderboard |
| `src/tp2025/jobs/extract_chinfo_to_landing.py` | Selecciona los top personajes y extrae sus perfiles a Parquet |
| `src/tp2025/jobs/load_chinfo_raw_to_db.py` | Carga la info de personajes en RAW |
| `src/tp2025/jobs/build_chinfo_cur.py` | Construye la tabla CUR de personajes |
| `src/tp2025/jobs/load_warehouse_redshift.py` | Lee CUR (DuckDB) y carga el star schema en Redshift |
| `src/tp2025/transforms/transform_leaderboard.py` | Lógica de casting y modelado para `cur_pvp_leaderboard` |
| `src/tp2025/transforms/transform_chinfo.py` | Lógica de casting y modelado para `cur_chinfo` |
| `src/tp2025/warehouse/connect_redshift.py` | Conexión a Redshift y search_path |
| `src/tp2025/warehouse/redshift_model.py` | DDL y cargas masivas (SCD2 de personajes y fact snapshot) |
| `src/tp2025/io/load_localdb.py` | Helper para conexión a DuckDB y ejecución SQL local |
| `src/tp2025/services/character_selection.py` | Selección de top personajes únicos desde CUR |
| `src/tp2025/services/ch_profile_client.py` | Requests concurrentes al endpoint de perfil de personaje |
| `docker-compose.yml` | Orquesta Postgres (metadatos) y el webserver/scheduler de Airflow |
| `Dockerfile` | Imagen custom de Airflow con el proyecto instalado via uv |
| `Makefile` | Shortcuts: `make env`, `make build`, `make init`, `make up`, `make down`, `make test` |
| `tests/` | Tests unitarios para autenticación y transformaciones |
