import pandas as pd
from typing import Dict, List


def detect_date_columns(df: pd.DataFrame) -> List[str]:
    """
    Автоматическое обнаружение колонок с датами.
    """
    date_columns = []
    date_keywords = ['date', 'time', 'day', 'month', 'year', 'created', 'updated', 'timestamp']

    for column in df.columns:
        col_lower = column.lower()
        if any(keyword in col_lower for keyword in date_keywords):
            date_columns.append(column)
        elif df[column].dtype == 'object':
            sample = df[column].dropna().head(5)
            if not sample.empty and sample.astype(str).str.match(r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}').any():
                date_columns.append(column)

    return date_columns


def infer_types(df: pd.DataFrame, type_hints: Dict[str, str] = None) -> pd.DataFrame:
    """
    Приведение типов данных с оптимизацией памяти.
    """
    df_copy = df.copy()

    print("\nПриведение типов данных:")

    for column in df_copy.columns:
        original_dtype = df_copy[column].dtype
        unique_count = df_copy[column].nunique()

        if unique_count <= 20 and df_copy[column].dtype == 'object':
            df_copy[column] = df_copy[column].astype('category')
            print(f"  {column}: object → category ({unique_count} уникальных)")

        elif df_copy[column].dtype == 'object':
            converted = pd.to_numeric(df_copy[column], errors='coerce')
            if not converted.isna().all():
                df_copy[column] = converted
                print(f"  {column}: object → numeric")
            else:
                df_copy[column] = df_copy[column].astype('string')
                print(f"  {column}: object → string")

        elif pd.api.types.is_integer_dtype(df_copy[column]):
            df_copy[column] = pd.to_numeric(df_copy[column], downcast='integer')
            print(f"  {column}: int → {df_copy[column].dtype}")

        elif pd.api.types.is_float_dtype(df_copy[column]):
            df_copy[column] = pd.to_numeric(df_copy[column], downcast='float')
            print(f"  {column}: float → {df_copy[column].dtype}")

    date_columns = detect_date_columns(df_copy)
    for col in date_columns:
        df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')
        print(f"  {col}: → datetime64")

    return df_copy


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка данных."""
    df_copy = df.copy()

    df_copy = df_copy.dropna(how='all')
    df_copy = df_copy.drop_duplicates()

    for col in df_copy.select_dtypes(include='object').columns:
        df_copy[col] = df_copy[col].str.strip()

    return df_copy


def transform(df: pd.DataFrame, type_hints: Dict[str, str] = None) -> pd.DataFrame:
    """
    Основная функция трансформации.
    """
    df = clean_data(df)
    df = infer_types(df, type_hints)

    memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"\n✓ Трансформация завершена")
    print(f"  Размер: {df.shape[0]} строк × {df.shape[1]} столбцов")
    print(f"  Память: {memory_usage:.2f} МБ")

    return df
