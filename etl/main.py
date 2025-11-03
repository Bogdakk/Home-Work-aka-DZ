"""
Главный модуль ETL пакета с CLI интерфейсом.
"""

import argparse
import sys
import sqlite3
import pandas as pd
from etl.extract import extract
from etl.transform import transform
from etl.load import load
from etl.validate import validate_output


def show_database_content(db_path: str = 'data/processed/data.db', table_name: str = 'processed_data') -> None:
    """
    Показывает содержимое БД
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        print("\n" + "="*70)
        print("СОДЕРЖИМОЕ SQLite БД")
        print("="*70 + "\n")

        print(f"Таблица: {table_name}")
        print(f"Столбцы ({len(columns)}):")
        for col in columns:
            col_name = col[1]
            col_type = col[2]
            print(f"  {col_name}: {col_type}")

        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        print(f"\nВсего строк: {row_count}\n")

        print("-"*70)
        print("Первые 10 строк из БД:")
        print("-"*70 + "\n")

        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 10", conn)
        print(df.to_string(index=False))
        print("\n" + "="*70 + "\n")

        conn.close()

    except Exception as e:
        print(f"Ошибка при чтении БД: {e}")


def run_etl(input_file: str = None,
            google_drive_id: str = None,
            postgresql_table: str = None,
            max_rows: int = 100) -> None:
    """
    Запускает полный ETL процесс
    """
    print("\nЗАПУСК ETL ПРОЦЕССА\n")

    try:
        # EXTRACT
        print("ЭТАП 1: EXTRACT")
        print("-"*70)
        df = extract(source_path=input_file, google_drive_id=google_drive_id)
        print(f"Загружено: {df.shape[0]} строк × {df.shape[1]} столбцов\n")

        # TRANSFORM
        print("ЭТАП 2: TRANSFORM")
        print("-"*70)
        df = transform(df)
        print()

        # VALIDATE
        print("ЭТАП 3: VALIDATE")
        print("-"*70)
        validate_output(df, verbose=False)
        print("Валидация пройдена успешно\n")

        # LOAD
        print("ЭТАП 4: LOAD")
        print("-"*70)
        results = load(
            df,
            sqlite_db_path='data/processed/data.db',
            postgresql_table=postgresql_table,
            parquet_path='data/processed/data.parquet',
            csv_path='data/processed/data.csv',
            feather_path='data/processed/data.feather',
            max_rows=max_rows,
            verbose=True
        )

        # Показываем содержимое БД
        show_database_content()

        print("ETL ПРОЦЕСС ЗАВЕРШЕН УСПЕШНО!\n")

    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """
    CLI интерфейс для ETL пакета
    """
    parser = argparse.ArgumentParser(
        description="ETL пакет для обработки данных",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python -m etl.main
  python -m etl.main --file data/input.csv
  python -m etl.main --google-drive-id YOUR_FILE_ID
  python -m etl.main --google-drive-id YOUR_FILE_ID --table my_table --max-rows 100
        """
    )

    # Если нет аргументов - используем Google Drive по умолчанию
    if len(sys.argv) == 1:
        sys.argv.extend(['--google-drive-id', '1cnduXIlbEZTrSB6DfOpV5ifAVzkWkHE9'])

    source_group = parser.add_mutually_exclusive_group(required=False)
    source_group.add_argument(
        '--file',
        type=str,
        help='Путь к локальному файлу (csv, xlsx, json)'
    )
    source_group.add_argument(
        '--google-drive-id',
        type=str,
        help='Google Drive FILE_ID для загрузки данных'
    )

    parser.add_argument(
        '--table',
        type=str,
        default=None,
        help='Название таблицы в PostgreSQL'
    )

    parser.add_argument(
        '--max-rows',
        type=int,
        default=100,
        help='Максимальное количество строк для БД (по умолчанию: 100)'
    )

    args = parser.parse_args()

    # Запуск ETL
    run_etl(
        input_file=args.file,
        google_drive_id=args.google_drive_id,
        postgresql_table=args.table,
        max_rows=args.max_rows
    )


if __name__ == "__main__":
    main()
