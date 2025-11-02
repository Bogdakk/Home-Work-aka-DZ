import sqlite3
import os
import pandas as pd
from sqlalchemy import create_engine


def load_credentials_from_sqlite(db_path="creds.db"):
    """
    Загружает учетные данные из SQLite базы данных creds.db
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT url, port, user, pass FROM access;")
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise Exception("Не найдены учетные данные в таблице access")

    url, port, user, password = row

    os.environ["DB_USER"] = user
    os.environ["DB_PASSWORD"] = password
    os.environ["DB_URL"] = url
    os.environ["DB_PORT"] = str(port)
    os.environ["DB_ROOT_BASE"] = "homeworks"


def get_engine():
    """
    Создает SQLAlchemy engine для подключения к PostgreSQL
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    url = os.getenv("DB_URL")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_ROOT_BASE")

    if not all([user, password, url, port, dbname]):
        raise Exception("Не все переменные среды заданы!")

    engine_url = f"postgresql+psycopg2://{user}:{password}@{url}:{port}/{dbname}"
    engine = create_engine(engine_url)
    return engine


def load_and_write_data(engine, table_name):
    """
    Загружает данные из feather файла и записывает в PostgreSQL
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    possible_paths = [
        os.path.join(script_dir, "dataset_converted.feather"),
        os.path.join(script_dir, "..", "dataset_converted.feather"),
        "dataset_converted.feather"
    ]

    data_path = None
    for path in possible_paths:
        if os.path.exists(path):
            data_path = path
            break

    if not data_path:
        raise FileNotFoundError("Файл dataset_converted.feather не найден!")

    df = pd.read_feather(data_path)
    df = df.head(100)

    df.to_sql(
        name=table_name,
        con=engine,
        schema="public",
        if_exists="replace",
        index=False
    )
    print(f"Данные успешно записаны в таблицу public.{table_name}")


if __name__ == "__main__":
    load_credentials_from_sqlite()
    engine = get_engine()
    table_name = "Korotkov"
    load_and_write_data(engine, table_name)

    df_check = pd.read_sql_table(table_name, con=engine, schema="public")
    print("Первые строки из таблицы:")
    print(df_check.head())
