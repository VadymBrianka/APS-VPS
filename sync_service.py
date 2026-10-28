import os
import glob
import json
import time
import logging
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Читання конфігурації з .env
DB_HOST = os.getenv("DB_HOST", "postgres_db")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "cloudfarms_db")
DB_USER = os.getenv("DB_USER", "cf_user")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CF_AUTH_URL = os.getenv("CF_AUTH_URL")
CF_DATA_URL = os.getenv("CF_DATA_URL")
CF_CLIENT_ID = os.getenv("CF_CLIENT_ID")
CF_CLIENT_SECRET = os.getenv("CF_CLIENT_SECRET")
CF_TOKEN_DURATION = int(os.getenv("CF_TOKEN_DURATION", 60))
INITIAL_SINCE = os.getenv("INITIAL_SINCE", "2026-10-25T00:00:00Z")
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", 300))

TABLE_SCHEMAS = {}


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def wait_for_postgres():
    """Очікує повної готовності сервера БД."""
    while True:
        try:
            conn = get_db_connection()
            conn.close()
            logging.info("Підключення до PostgreSQL встановлено успішно.")
            break
        except psycopg2.OperationalError:
            logging.info("Очікування запуску PostgreSQL...")
            time.sleep(2)


def map_type(db_type_name: str, precision: int = None, scale: int = None) -> str:
    """Мапує типи з JSON-метаданих у коректні типи PostgreSQL."""
    dt = db_type_name.lower()
    if dt == "bigserial":
        return "BIGINT"  # Значення постачаються з API, автоінкремент заважатиме upsert
    if dt == "int4":
        return "INTEGER"
    if dt == "int8":
        return "BIGINT"
    if dt == "int2":
        return "SMALLINT"
    if dt == "varchar":
        return f"VARCHAR({precision})" if precision else "VARCHAR"
    if dt == "bpchar":
        return f"CHAR({precision})" if precision else "CHAR"
    if dt == "numeric":
        return f"NUMERIC({precision},{scale})" if (precision and scale is not None) else "NUMERIC"
    if dt == "float8":
        return "DOUBLE PRECISION"
    if dt == "bool":
        return "BOOLEAN"
    if dt == "timestamptz":
        return "TIMESTAMPTZ"
    if dt == "timestamp":
        return "TIMESTAMP"
    if dt == "date":
        return "DATE"
    if dt == "text":
        return "TEXT"
    return "TEXT"


def init_database():
    """Створює службові таблиці та всі таблиці за JSON-схемами."""
    global TABLE_SCHEMAS
    schema_files = glob.glob("schemas/*.json")
    for file_path in schema_files:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for table_def in data:
                TABLE_SCHEMAS[table_def["name"]] = table_def

    conn = get_db_connection()
    cur = conn.cursor()

    # Таблиця збереження стану інкрементальної синхронізації
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_state (
            pipeline_name VARCHAR(100) PRIMARY KEY,
            next_since VARCHAR(100) NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Створення цільових таблиць
    for table_name, table_def in TABLE_SCHEMAS.items():
        cols_sql = []
        for col in table_def["columns"]:
            c_name = col["name"]
            c_type = map_type(col["dbTypeName"], col.get("precision"), col.get("scale"))
            cols_sql.append(f'"{c_name}" {c_type}')

        keys = table_def.get("key", [])
        if keys:
            pk_cols = ", ".join([f'"{k}"' for k in keys])
            cols_sql.append(f"PRIMARY KEY ({pk_cols})")

        create_stmt = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  ' + ",\n  ".join(cols_sql) + "\n);"
        cur.execute(create_stmt)

    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Схеми {len(TABLE_SCHEMAS)} таблиць успішно ініціалізовано в PostgreSQL.")


def get_bearer_token() -> str:
    """Виконує запит авторизації та повертає JWT Bearer-токен."""
    payload = {
        "clientId": CF_CLIENT_ID,
        "clientSecret": CF_CLIENT_SECRET,
        "duration": CF_TOKEN_DURATION
    }
    response = requests.post(CF_AUTH_URL, json=payload, timeout=20)
    response.raise_for_status()
    data = response.json()
    return data["token"]


def get_current_since() -> str:
    """Отримує збережений nextSince з БД або дефолтний INITIAL_SINCE."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT next_since FROM sync_state WHERE pipeline_name = 'cloudfarms_main';")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else INITIAL_SINCE


def save_next_since(next_since: str):
    """Зберігає отриманий новий nextSince в БД."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sync_state (pipeline_name, next_since, updated_at)
        VALUES ('cloudfarms_main', %s, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name)
        DO UPDATE SET next_since = EXCLUDED.next_since, updated_at = CURRENT_TIMESTAMP;
    """, (next_since,))
    conn.commit()
    cur.close()
    conn.close()


def process_delta_table(conn, table_name: str, payload: dict):
    """Виконує Upsert для доданих/оновлених та Delete для видалених записів."""
    table_def = TABLE_SCHEMAS.get(table_name)
    if not table_def:
        return

    cur = conn.cursor()
    keys = table_def.get("key", [])
    upserted_rows = payload.get("upserted", [])
    deleted_rows = payload.get("deleted", [])

    # 1. Обробка UPSERTED
    if upserted_rows and keys:
        columns = [c["name"] for c in table_def["columns"]]
        col_identifiers = [sql.Identifier(col) for col in columns]
        key_identifiers = [sql.Identifier(k) for k in keys]
        non_key_cols = [c for c in columns if c not in keys]

        if non_key_cols:
            update_assignments = [
                sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                for c in non_key_cols
            ]
            conflict_action = sql.SQL("DO UPDATE SET {updates}").format(
                updates=sql.SQL(", ").join(update_assignments)
            )
        else:
            conflict_action = sql.SQL("DO NOTHING")

        query = sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols})
            {conflict_action}
        """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(col_identifiers),
            placeholders=sql.SQL(", ").join([sql.Placeholder() for _ in columns]),
            conflict_cols=sql.SQL(", ").join(key_identifiers),
            conflict_action=conflict_action
        )

        records_data = []
        for item in upserted_rows:
            records_data.append(tuple(item.get(col) for col in columns))

        execute_batch(cur, query, records_data, page_size=200)

    # 2. Обробка DELETED
    if deleted_rows and keys:
        where_clauses = [
            sql.SQL("{col} = {placeholder}").format(
                col=sql.Identifier(k),
                placeholder=sql.Placeholder()
            )
            for k in keys
        ]
        delete_query = sql.SQL("""
            DELETE FROM {table} WHERE {where_conditions}
        """).format(
            table=sql.Identifier(table_name),
            where_conditions=sql.SQL(" AND ").join(where_clauses)
        )

        delete_tuples = []
        for item in deleted_rows:
            if all(k in item for k in keys):
                delete_tuples.append(tuple(item[k] for k in keys))

        if delete_tuples:
            execute_batch(cur, delete_query, delete_tuples, page_size=200)

    cur.close()


def sync_cycle():
    """Повний цикл: токен -> вибірка дельти -> оновлення БД -> збереження nextSince."""
    try:
        since_val = get_current_since()
        logging.info(f"Запуск синхронізації. Параметр since={since_val}")

        token = get_bearer_token()
        headers = {"Authorization": f"Bearer {token}"}
        params = {"since": since_val}

        response = requests.get(CF_DATA_URL, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        next_since = data.get("nextSince")
        if not next_since:
            logging.warning("Відповідь API не містить nextSince. Пропуск оновлення.")
            return

        conn = get_db_connection()
        for key, value in data.items():
            if isinstance(value, dict) and ("upserted" in value or "deleted" in value):
                process_delta_table(conn, key, value)

        conn.commit()
        conn.close()

        save_next_since(next_since)
        logging.info(f"Синхронізація успішна. Новий nextSince збережено: {next_since}")

    except Exception as e:
        logging.error(f"Помилка під час синхронізації: {e}", exc_info=True)


if __name__ == "__main__":
    wait_for_postgres()
    init_database()
    logging.info(f"Сервіс запущено. Періодичність виконання: кожні {SYNC_INTERVAL_SECONDS} секунд.")

    while True:
        sync_cycle()
        time.sleep(SYNC_INTERVAL_SECONDS)