"""
Скрипт для проверки что сохранилось в БД
"""

import sqlite3
import pandas as pd
from pathlib import Path


def check_sqlite_database(db_path: str = 'data/processed/data.db') -> None:
    """
    Проверяет содержимое SQLite базы данных
    """
    if not Path(db_path).exists():
        print(f"❌ БД не найдена: {db_path}")
        return

    print(f"\n{'=' * 70}")
    print(f"📊 ПРОВЕРКА SQLite БД: {db_path}")
    print(f"{'=' * 70}\n")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Получаем список всех таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    if not tables:
        print("❌ В БД нет таблиц")
        conn.close()
        return

    for table in tables:
        table_name = table[0]
        print(f"📋 Таблица: {table_name}")
        print(f"{'-' * 70}")

        # Информация о таблице
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        print(f"Столбцы ({len(columns)}):")
        for col in columns:
            col_id, col_name, col_type, not_null, default_val, primary_key = col
            nullable = "NOT NULL" if not_null else "NULL"
            print(f"  • {col_name} ({col_type}) {nullable}")

        # Количество строк
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        print(f"\nКоличество строк: {row_count}\n")

        # Первые 10 строк
        print(f"Первые 10 строк:")
        df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 10", conn)
        print(df.to_string(index=False))
        print()

        # Статистика
        print(f"Статистика:")
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        if len(numeric_cols) > 0:
            print(df[numeric_cols].describe().to_string())
        print(f"\n{'=' * 70}\n")

    conn.close()
    print("✅ Проверка БД завершена!\n")


if __name__ == "__main__":
    check_sqlite_database()
