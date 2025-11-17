from __future__ import annotations

from datetime import date

from tp2025.io.load_localdb import get_connection, run_sql

RAW_TABLE = "raw_chinfo"
CUR_TABLE = "cur_chinfo"


def create_cur_chinfo(processing_date: str | None = None) -> None:
    """
    Construye la tabla CUR de información de personajes a partir de la tabla RAW
    para una fecha de proceso dada.

    - Toma datos de raw_chinfo.
    - Filtra por fecha_proceso (por defecto, la fecha de hoy en formato YYYYMMDD).
    - Castea tipos y renombra columnas según el modelo de negocio.
    - Crea o reemplaza la tabla cur_chinfo.
    """
    if processing_date is None:
        processing_date = date.today().strftime("%Y%m%d")

    conn = get_connection()
    try:
        query = f"""
        CREATE OR REPLACE TABLE {CUR_TABLE} AS
        SELECT 
            CAST(id AS BIGINT)      AS char_id,
            name                    AS char_name,
            realm_slug              AS slug_name,
            faction                 AS faction_type,
            class                   AS class_name,
            spec                    AS current_spec,
            CAST(a_ilvl AS INT)     AS average_item_level,
            CAST(e_ilvl AS INT)     AS equipped_item_level,
            fecha_proceso
        FROM {RAW_TABLE}
        WHERE fecha_proceso = '{processing_date}';
        """
        run_sql(conn, query)
        print(
            f"[transform_chinfo] Tabla {CUR_TABLE} generada "
            f"para fecha_proceso={processing_date}"
        )
    finally:
        conn.close()