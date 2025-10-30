import pandas as pd
import sqlite3
import os
from pathlib import Path
from sqlalchemy import create_engine
from typing import Any
from typing import Optional


# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def load_credentials_from_sqlite(
    creds_db_path: str = "creds.db",
    script_dir: Optional[Path] = None
) -> None:
    """
    Загружает учетные данные из SQLite и сохраняет в переменные окружения.
    """
    if script_dir is None:
        script_dir = Path(__file__).parent.resolve()
    full_path = script_dir / creds_db_path

    if not full_path.exists():
        raise FileNotFoundError(f"Файл с учетными данными не найден: {full_path}")

    print(f"Чтение учетных данных из: {full_path}")
    try:
        conn = sqlite3.connect(full_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url, port, user, pass FROM access;")
        row = cursor.fetchone()
        conn.close()

        if not row:
            raise ValueError("Таблица 'access' пуста или не содержит данных")

        url, port, user, password = row

        # Устанавливаем переменные окружения
        os.environ["DB_USER"] = user
        os.environ["DB_PASSWORD"] = password
        os.environ["DB_URL"] = url
        os.environ["DB_PORT"] = str(port)
        os.environ["DB_ROOT_BASE"] = "homeworks"

        print("Учетные данные успешно загружены в переменные окружения")
    except Exception as e:
        raise RuntimeError(f"Ошибка чтения creds.db: {e}")


def get_engine() -> Any:
    """
    Создаёт SQLAlchemy engine для PostgreSQL.
    """
    required = ["DB_USER", "DB_PASSWORD", "DB_URL", "DB_PORT", "DB_ROOT_BASE"]
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"Не заданы переменные окружения: {', '.join(missing)}")

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    url = os.getenv("DB_URL")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_ROOT_BASE")

    engine_url = f"postgresql+psycopg2://{user}:{password}@{url}:{port}/{dbname}"
    print(f"Подключение к PostgreSQL: {url}:{port}/{dbname}")
    return create_engine(engine_url, pool_pre_ping=True)


def load_to_postgres(
    df: pd.DataFrame,
    table_name: str = "Korotkov",
    max_rows: int = 100,
    schema: str = "public"
) -> None:
    """
    Загружает ограниченное количество строк в PostgreSQL.
    """
    print(f"Загрузка в PostgreSQL: таблица '{schema}.{table_name}', макс. {max_rows} строк")
    load_credentials_from_sqlite()
    engine = get_engine()

    df_limited = df.head(max_rows)
    try:
        df_limited.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi"
        )
        print(f"Успешно загружено {len(df_limited)} строк в {schema}.{table_name}")
    except Exception as e:
        raise RuntimeError(f"Ошибка загрузки в PostgreSQL: {e}")


def load_processed(
    relative_path: str = "data/processed/dataset_processed.csv"
) -> pd.DataFrame:
    """
    Загружает обработанные данные из CSV.
    """
    script_dir = Path(__file__).parent.resolve()
    full_path = script_dir / relative_path

    if not full_path.exists():
        raise FileNotFoundError(f"Файл не найден: {full_path}")

    print(f"Загрузка обработанных данных из: {full_path}")
    try:
        df = pd.read_csv(full_path)
        print(f"Загружено: {df.shape[0]} строк, {df.shape[1]} колонок")
        return df
    except Exception as e:
        raise RuntimeError(f"Ошибка чтения CSV: {e}")


def save_to_parquet(
    df: pd.DataFrame,
    relative_path: str = "data/processed/dataset_processed.parquet"
) -> None:
    """
    Сохраняет DataFrame в Parquet.
    """
    script_dir = Path(__file__).parent.resolve()
    full_path = script_dir / relative_path

    try:
        os.makedirs(full_path.parent, exist_ok=True)
        df.to_parquet(full_path, index=False, engine="pyarrow")
        print(f"Данные сохранены в Parquet: {full_path}")
    except Exception as e:
        raise RuntimeError(f"Ошибка сохранения Parquet: {e}")


# === ОСНОВНАЯ ФУНКЦИЯ ===

def load(
    processed_csv_path: str = "data/processed/dataset_processed.csv",
    parquet_path: str = "data/processed/dataset_processed.parquet",
    table_name: str = "Korotkov",
    max_rows: int = 100
) -> pd.DataFrame:
    """
    Основная функция load:
    1. Загружает обработанные данные
    2. Сохраняет в Parquet
    3. Загружает в PostgreSQL (ограниченное кол-во строк)
    """
    print("Запуск этапа LOAD...")

    # 1. Загрузка данных
    df = load_processed(processed_csv_path)

    # 2. Сохранение в Parquet
    save_to_parquet(df, parquet_path)

    # 3. Загрузка в PostgreSQL
    load_to_postgres(df, table_name=table_name, max_rows=max_rows)

    print("Этап LOAD завершён успешно!")
    return df


# === ТОЧКА ВХОДА ===
if __name__ == "__main__":
    try:
        df_loaded = load()
        print(f"\nПервые 2 строки загруженных данных:")
        print(df_loaded.head(2))
        print(f"\nParquet сохранён: {Path('data/processed/dataset_processed.parquet').resolve()}")
    except Exception as e:
        print(f"Ошибка в load: {e}")