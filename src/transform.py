import pandas as pd
from pathlib import Path
from typing import List
import os


def detect_date_columns(df: pd.DataFrame) -> List[str]:
    """
    Определяет колонки, которые могут быть датами (по названию или формату).
    """
    date_keywords = ['date', 'time', 'day', 'month', 'year', 'created', 'updated', 'timestamp']
    date_columns = []

    for col in df.columns:
        col_lower = col.lower()

        # По ключевым словам
        if any(kw in col_lower for kw in date_keywords):
            date_columns.append(col)
            continue

        # По формату (только для object)
        if df[col].dtype == 'object':
            sample = df[col].dropna().head(10).astype(str)
            patterns = [
                r'^\d{4}-\d{2}-\d{2}',           # 2023-12-25
                r'^\d{2}/\d{2}/\d{4}',           # 25/12/2023
                r'^\d{4}/\d{2}/\d{2}',           # 2023/12/25
                r'^\d{1,2} \w{3} \d{4}',         # 25 Dec 2023
                r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}' # 2023-12-25 14:30
            ]
            if sample.str.match('|'.join(patterns)).any():
                date_columns.append(col)

    return date_columns


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приводит типы данных к оптимальным:
    - category для низкой кардинальности
    - numeric (int/float) при возможности
    - string для текста
    - datetime для дат
    """
    df_conv = df.copy()

    for col in df_conv.columns:
        if col not in df_conv.columns:
            continue

        series = df_conv[col]
        unique_count = series.nunique()
        total_count = len(series)

        # Категории: мало уникальных значений
        if unique_count <= 20 and series.dtype == 'object':
            df_conv[col] = series.astype('category')
        
        # Попытка преобразовать object в число
        elif series.dtype == 'object':
            numeric = pd.to_numeric(series, errors='coerce')
            if not numeric.isna().all():
                df_conv[col] = numeric
                # Даункаст чисел
                if pd.api.types.is_integer_dtype(numeric):
                    df_conv[col] = pd.to_numeric(numeric, downcast='integer')
                elif pd.api.types.is_float_dtype(numeric):
                    df_conv[col] = pd.to_numeric(numeric, downcast='float')
            else:
                df_conv[col] = series.astype('string')

        # Даункаст для чисел
        elif pd.api.types.is_integer_dtype(series):
            df_conv[col] = pd.to_numeric(series, downcast='integer')
        elif pd.api.types.is_float_dtype(series):
            df_conv[col] = pd.to_numeric(series, downcast='float')
        elif series.dtype == 'bool':
            df_conv[col] = series.astype('boolean')

    # Преобразование дат
    date_cols = detect_date_columns(df_conv)
    for col in date_cols:
        df_conv[col] = pd.to_datetime(df_conv[col], errors='coerce')

    return df_conv


def load_raw(relative_path: str = "data/raw/dataset_raw.csv") -> pd.DataFrame:
    """
    Загружает сырые данные относительно расположения скрипта.
    """
    script_dir = Path(__file__).parent.resolve()
    full_path = script_dir / relative_path

    if not full_path.exists():
        raise FileNotFoundError(f"Файл не найден: {full_path}")

    print(f"Загрузка сырых данных из: {full_path}")
    try:
        df = pd.read_csv(full_path)
        print(f"Загружено: {df.shape[0]} строк, {df.shape[1]} колонок")
        return df
    except Exception as e:
        raise RuntimeError(f"Ошибка чтения CSV: {e}")


def save_processed(df: pd.DataFrame, relative_path: str = "data/processed/dataset_processed.csv") -> None:
    """
    Сохраняет обработанные данные.
    """
    script_dir = Path(__file__).parent.resolve()
    full_path = script_dir / relative_path

    try:
        os.makedirs(full_path.parent, exist_ok=True)
        df.to_csv(full_path, index=False, encoding='utf-8')
        print(f"Обработанные данные сохранены: {full_path}")
    except Exception as e:
        raise RuntimeError(f"Ошибка сохранения {full_path}: {e}")


def transform(raw_path: str = "data/raw/dataset_raw.csv",
              processed_path: str = "data/processed/dataset_processed.csv") -> pd.DataFrame:
    """
    Основная функция transform: загрузка → трансформация → сохранение.
    """
    print("Запуск трансформации данных...")
    df = load_raw(raw_path)
    print("Трансформация: приведение типов, дат, категорий...")
    
    df_transformed = convert_data_types(df)
    
    print(f"Типы после трансформации:\n{df_transformed.dtypes}")
    print(f"Пропуски после преобразования:\n{df_transformed.isna().sum()}")

    save_processed(df_transformed, processed_path)
    return df_transformed


# === ТОЧКА ВХОДА ===
if __name__ == "__main__":
    try:
        df_processed = transform()
        print("\nТрансформация завершена успешно!")
        print(f"Первые 2 строки обработанных данных:")
        print(df_processed.head(2))
        print(f"\nФайл сохранён: {Path('data/processed/dataset_processed.csv').resolve()}")
    except Exception as e:
        print(f"Ошибка в transform: {e}")