FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential libpq-dev git \
 && rm -rf /var/lib/apt/lists/*

# --- Isolated dbt environment ------------------------------------------------
# dbt deps are kept OUT of the Airflow Python env to avoid version conflicts.
# DAG tasks call /opt/dbt-venv/bin/dbt directly.
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-clickhouse~=1.8.0" \
 && chmod -R a+rx /opt/dbt-venv

USER airflow
# --- Airflow env: lightweight deps for the Extract & Load + seed scripts -----
# NOTE: do NOT install SQLAlchemy here — Airflow pins 1.4.x and our EL works
# fine on it (pandas.read_sql + create_engine).
RUN pip install --no-cache-dir \
      clickhouse-connect==0.7.19 \
      psycopg2-binary==2.9.9 \
      pandas==2.2.2 \
      Faker==25.2.0
