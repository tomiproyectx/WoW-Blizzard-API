# Imagen base oficial de Airflow con Python 3.10
FROM apache/airflow:2.11.0-python3.10

########################
# 1) Dependencias de sistema (como root)
########################
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

########################
# 2) Cambiamos a usuario airflow para todo lo de Python
########################
USER airflow

########################
# 3) Directorio limpio para el proyecto
########################
WORKDIR /opt/airflow/project

########################
# 4) Instalamos uv como usuario airflow
########################
RUN pip install --no-cache-dir uv

########################
# 5) Copiamos metadata del proyecto
########################
COPY --chown=airflow:airflow pyproject.toml uv.lock README.md ./

########################
# 6) Instalamos el paquete del proyecto con uv
########################
# Esto construye e instala el paquete "blizzard" y sus dependencias
RUN uv pip install .

########################
# 7) Copiamos el código fuente
########################
COPY --chown=airflow:airflow src ./src

########################
# 8) Volvemos al directorio estándar de Airflow y copiamos los DAGs
########################
WORKDIR /opt/airflow

COPY --chown=airflow:airflow dags ./dags

########################
# 9) Hacemos visible tp2025 al intérprete de Python
########################
ENV PYTHONPATH=/opt/airflow/project/src
