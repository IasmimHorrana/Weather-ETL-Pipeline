# Cria o banco de dados separado que o Airflow usará como
# seu metadata store 
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airflow_db'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec

    GRANT ALL PRIVILEGES ON DATABASE airflow_db TO $POSTGRES_USER;
EOSQL

echo "[✔] airflow_db criado com sucesso."
