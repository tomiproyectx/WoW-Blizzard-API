# TP 2025 - Blizzard Data Pipeline
Proyecto del trabajo integrador de la Diplomatura en Cloud Data Engineering.

Resumen: 

Este proyecto implementa un pipeline de datos que extrae información desde las APIs públicas de Blizzard utilizando autenticación OAuth2 (Client Credentials). El pipeline obtiene datos del PvP Leaderboard y luego realiza llamadas adicionales para recolectar información detallada de un subconjunto de personajes.

Los datos obtenidos se almacenan temporalmente en formato Parquet y en una base local SQLite. Posteriormente, se transforman y se cargan en un esquema analítico dentro de AWS Redshift.

La ejecución del flujo completo se orquesta con Apache Airflow, desplegado mediante Docker. El repositorio incluye estructura modular en Python, pruebas unitarias, un workflow de CI en GitHub Actions y manejo seguro de credenciales mediante archivos de entorno.