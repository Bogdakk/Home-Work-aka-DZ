import pandas as pd
import sqlite3
from pathlib import Path
from typing import Union, Optional, Dict
import logging
import os

try:
    from sqlalchemy import create_engine

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_credentials_from_sqlite(db_path: str = "creds.db") -> Dict[str, str]:
    """Загружает учетные данные из SQLite базы данных."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url, port, user, pass FROM access;")
        row = cursor.fetchone()
        conn.close()

        if not row:
            raise ValueError("Не найдены учетные данные в таблице access")

        url, port, user, password = row

        return {
            "user": user,
            "password": password,
            "url": url,
            "port": str(port),
            "dbname": "homeworks"
        }

    except Exception as e:
        logger.error(f"Ошибка загрузки credentials: {e}")
        raise


def create_postgresql_engine(credentials: Dict[str, str]):
    """Создает SQLAlchemy engine для PostgreSQL."""
    if not SQLALCHEMY_AVAILABLE:
        raise ImportError("SQLAlchemy не установлен. Установите: pip install sqlalchemy psycopg2")

    user = credentials.get("user")
    password = credentials.get("password")
    url = credentials.get("url")
    port = credentials.get("port")
    dbname = credentials.get("dbname")

    if not all([user, password, url, port, dbname]):
        raise ValueError("Неполные credentials для подключения")

    engine_url = f"postgresql+psycopg2://{user}:{password}@{url}:{port}/{dbname}"

    try:
        engine = create_engine(engine_url)
        with engine.connect() as conn:
            logger.info("✓ Подключение к PostgreSQL успешно")
        return engine
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise


def load_to_postgresql(df: pd.DataFrame,
                       table_name: str = "processed_data",
                       schema: str = "public",
                       max_rows: int = 100,
                       if_exists: str = 'replace',
                       credentials_path: str = "creds.db") -> bool:
    """Загрузка данных в PostgreSQL БД."""
    try:
        df_limited = df.head(max_rows)
        actual_rows = len(df_limited)

        credentials = load_credentials_from_sqlite(credentials_path)
        engine = create_postgresql_engine(credentials)

        df_limited.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )

        logger.info(f"✓ {actual_rows} строк загружено в PostgreSQL: {table_name}")
        print(f"✓ Загружено в PostgreSQL: {actual_rows} строк")
        print(f"  Таблица: {schema}.{table_name}")

        check_df = pd.read_sql_table(table_name, con=engine, schema=schema)
        print(f"  Проверка: {len(check_df)} строк в таблице")

        engine.dispose()
        return True

    except Exception as e:
        logger.error(f"Ошибка при загрузке в PostgreSQL: {e}")
        print(f"❌ Ошибка загрузки в PostgreSQL: {e}")
        return False


def setup_sqlite_database(db_path: str, table_name: str = 'processed_data') -> sqlite3.Connection:
    """Инициализация SQLite БД."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    return conn


def validate_sqlite_write(conn: sqlite3.Connection, table_name: str,
                          expected_rows: int) -> bool:
    """Проверка успешной записи в SQLite БД."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    actual_rows = cursor.fetchone()[0]

    if actual_rows != expected_rows:
        logger.warning(
            f"Несоответствие количества строк: "
            f"ожидалось {expected_rows}, получено {actual_rows}"
        )
        return False

    return True


def load_to_sqlite(df: pd.DataFrame,
                   db_path: str = 'data/processed/data.db',
                   table_name: str = 'processed_data',
                   max_rows: int = 100,
                   if_exists: str = 'replace') -> bool:
    """Загрузка данных в SQLite БД."""
    try:
        df_limited = df.head(max_rows)
        actual_rows = len(df_limited)

        conn = setup_sqlite_database(db_path, table_name)

        df_limited.to_sql(
            table_name,
            conn,
            if_exists=if_exists,
            index=False,
            chunksize=1000
        )

        if validate_sqlite_write(conn, table_name, actual_rows):
            logger.info(f"✓ {actual_rows} строк загружено в SQLite: {db_path}")
            print(f"✓ Загружено в SQLite: {actual_rows} строк")

        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        print(f"  Столбцы: {', '.join([col[1] for col in columns])}")

        conn.close()
        return True

    except Exception as e:
        logger.error(f"Ошибка при загрузке в SQLite: {e}")
        print(f"❌ Ошибка загрузки в SQLite: {e}")
        return False


def load_to_parquet(df: pd.DataFrame,
                    output_path: str = 'data/processed/data.parquet',
                    compression: str = 'snappy') -> bool:
    """Сохранение данных в Parquet."""
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output_path, index=False, compression=compression)

        file_size = Path(output_path).stat().st_size / 1024 / 1024
        logger.info(f"✓ Данные сохранены в {output_path}")
        print(f"✓ Сохранено в Parquet: {output_path}")
        print(f"  Размер файла: {file_size:.2f} МБ")

        return True

    except Exception as e:
        logger.error(f"Ошибка при сохранении в Parquet: {e}")
        print(f"❌ Ошибка сохранения в Parquet: {e}")
        return False


def load_to_csv(df: pd.DataFrame,
                output_path: str = 'data/processed/data.csv') -> bool:
    """Сохранение данных в CSV."""
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(output_path, index=False, encoding='utf-8')

        file_size = Path(output_path).stat().st_size / 1024 / 1024
        logger.info(f"✓ Данные сохранены в {output_path}")
        print(f"✓ Сохранено в CSV: {output_path}")
        print(f"  Размер файла: {file_size:.2f} МБ")

        return True

    except Exception as e:
        logger.error(f"Ошибка при сохранении в CSV: {e}")
        print(f"❌ Ошибка сохранения в CSV: {e}")
        return False


def load_to_feather(df: pd.DataFrame,
                    output_path: str = 'data/processed/data.feather') -> bool:
    """Сохранение данных в Feather."""
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        df.to_feather(output_path)

        file_size = Path(output_path).stat().st_size / 1024 / 1024
        logger.info(f"✓ Данные сохранены в {output_path}")
        print(f"✓ Сохранено в Feather: {output_path}")
        print(f"  Размер файла: {file_size:.2f} МБ")

        return True

    except Exception as e:
        logger.error(f"Ошибка при сохранении в Feather: {e}")
        print(f"❌ Ошибка сохранения в Feather: {e}")
        return False


def generate_load_summary(results: dict) -> str:
    """Генерация итогового отчета."""
    summary = "\n" + "=" * 60 + "\n"
    summary += "ИТОГОВЫЙ ОТЧЕТ О ЗАГРУЗКЕ\n"
    summary += "=" * 60 + "\n\n"

    successful = sum(1 for v in results.values() if v)
    total = len(results)

    summary += f"Успешно: {successful}/{total}\n\n"

    for format_name, status in results.items():
        status_str = "✓ OK" if status else "❌ Ошибка"
        summary += f"{format_name:20} {status_str}\n"

    summary += "\n" + "=" * 60 + "\n"

    return summary


def load(df: pd.DataFrame,
         sqlite_db_path: Optional[str] = None,
         postgresql_table: Optional[str] = None,
         postgresql_creds: str = "creds.db",
         parquet_path: Optional[str] = None,
         csv_path: Optional[str] = None,
         feather_path: Optional[str] = None,
         max_rows: int = 100,
         verbose: bool = True) -> dict:
    """
    Основная функция загрузки во все форматы.
    """
    results = {}

    print("\n" + "=" * 60)
    print("📤 ЭТАП 4: LOAD (Загрузка)")
    print("=" * 60 + "\n")

    if sqlite_db_path:
        print("Загрузка в SQLite БД...")
        results['SQLite'] = load_to_sqlite(df, sqlite_db_path, max_rows=max_rows)
        print()

    if postgresql_table:
        print("Загрузка в PostgreSQL БД...")
        results['PostgreSQL'] = load_to_postgresql(
            df,
            table_name=postgresql_table,
            max_rows=max_rows,
            credentials_path=postgresql_creds
        )
        print()

    if parquet_path:
        print("Загрузка в Parquet...")
        results['Parquet'] = load_to_parquet(df, parquet_path)
        print()

    if csv_path:
        print("Загрузка в CSV...")
        results['CSV'] = load_to_csv(df, csv_path)
        print()

    if feather_path:
        print("Загрузка в Feather...")
        results['Feather'] = load_to_feather(df, feather_path)
        print()

    if verbose and results:
        summary = generate_load_summary(results)
        print(summary)

    return results
