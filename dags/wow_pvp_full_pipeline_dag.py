from __future__ import annotations

import sys
from pathlib import Path
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tp2025.jobs.extract_leaderboard_to_landing import (
    run_extract_leaderboard_to_landing,
)
from tp2025.jobs.load_leaderboard_raw_to_db import (
    run_load_leaderboard_raw_to_db,
)
from tp2025.jobs.build_leaderboard_cur import (
    run_build_leaderboard_cur,
)
from tp2025.jobs.extract_chinfo_to_landing import (
    run_extract_chinfo_to_landing,
)
from tp2025.jobs.load_chinfo_raw_to_db import (
    run_load_chinfo_raw_to_db,
)
from tp2025.jobs.build_chinfo_cur import (
    run_build_chinfo_cur,
)

# Import de la carga a Redshift
from tp2025.jobs.load_warehouse_redshift import main as load_warehouse_main


# ---------- Helpers ----------

def set_blizzard_env_vars() -> None:
    """
    Lee credenciales de Blizzard desde Airflow Variables
    y las exporta a variables de entorno para que las use auth_client.py.

    Variables esperadas en Airflow:
      - BLIZZARD_CLIENT_ID
      - BLIZZARD_CLIENT_SECRET
      - BLIZZARD_REGION (opcional, por defecto 'us')
    """
    client_id = Variable.get("BLIZZARD_CLIENT_ID", default_var=None)
    client_secret = Variable.get("BLIZZARD_CLIENT_SECRET", default_var=None)
    region = Variable.get("BLIZZARD_REGION", default_var="us")

    if not client_id or not client_secret:
        raise ValueError(
            "Faltan Airflow Variables 'BLIZZARD_CLIENT_ID' y/o "
            "'BLIZZARD_CLIENT_SECRET'. Configurarlas en Admin > Variables."
        )

    os.environ["BLIZZARD_CLIENT_ID"] = client_id
    os.environ["BLIZZARD_CLIENT_SECRET"] = client_secret
    os.environ["BLIZZARD_REGION"] = region


def run_load_warehouse(processing_date: str | None = None) -> None:
    """
    Wrapper para llamar a load_warehouse.main desde Airflow.

    processing_date se pasa en formato YYYYMMDD, consistente con fecha_proceso.
    Si no se pasa, main usa la fecha de hoy.
    """
    load_warehouse_main(processing_date=processing_date)


# ---------- Definición del DAG ----------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wow_pvp_full_pipeline",
    description="Pipeline diario completo: Blizzard API -> DuckDB (raw/cur) -> Redshift",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval="0 6 * * *",  # diario
    catchup=False,
    max_active_runs=1,
    tags=["wow", "tp2025", "blizzard"],
) as dag:

    # 0) Seteo de credenciales de Blizzard en variables de entorno
    t_set_blizzard_env = PythonOperator(
        task_id="set_blizzard_env_vars",
        python_callable=set_blizzard_env_vars,
    )

    # 1) EXTRAER LEADERBOARD -> LANDING (parquet)
    t_extract_leaderboard = PythonOperator(
        task_id="extract_leaderboard_to_landing",
        python_callable=run_extract_leaderboard_to_landing,
    )

    # 2) CARGAR LEADERBOARD -> RAW (DuckDB)
    t_load_leaderboard_raw = PythonOperator(
        task_id="load_leaderboard_raw_to_db",
        python_callable=run_load_leaderboard_raw_to_db,
    )

    # 3) CONSTRUIR LEADERBOARD -> CUR (DuckDB)
    t_build_leaderboard_cur = PythonOperator(
        task_id="build_leaderboard_cur",
        python_callable=run_build_leaderboard_cur,
    )

    # 4) EXTRAER CHINFO -> LANDING (parquet)
    t_extract_chinfo = PythonOperator(
        task_id="extract_chinfo_to_landing",
        python_callable=run_extract_chinfo_to_landing,
    )

    # 5) CARGAR CHINFO -> RAW (DuckDB)
    t_load_chinfo_raw = PythonOperator(
        task_id="load_chinfo_raw_to_db",
        python_callable=run_load_chinfo_raw_to_db,
    )

    # 6) CONSTRUIR CHINFO -> CUR (DuckDB)
    t_build_chinfo_cur = PythonOperator(
        task_id="build_chinfo_cur",
        python_callable=run_build_chinfo_cur,
    )

    # 7) CARGA / REFRESH DEL MODELO EN REDSHIFT
    t_load_redshift = PythonOperator(
        task_id="load_redshift_model",
        python_callable=run_load_warehouse,
        op_kwargs={
            "processing_date": "{{ ds_nodash }}",
        },
    )

    # ---------- Dependencias ----------

    t_set_blizzard_env >> t_extract_leaderboard >> t_load_leaderboard_raw >> t_build_leaderboard_cur

    t_build_leaderboard_cur >> t_extract_chinfo >> t_load_chinfo_raw >> t_build_chinfo_cur

    t_build_chinfo_cur >> t_load_redshift
