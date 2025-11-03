import pandas as pd
from typing import List, Tuple, Dict


def check_nulls(df: pd.DataFrame, threshold: float = 0.5) -> Tuple[bool, str]:
    """Проверка количества NULL значений."""
    null_ratio = df.isnull().sum() / len(df)
    bad_cols = null_ratio[null_ratio > threshold]

    if len(bad_cols) > 0:
        msg = "⚠ Столбцы с >50% NULL значений:\n"
        for col, ratio in bad_cols.items():
            msg += f"    {col}: {ratio * 100:.1f}%\n"
        return False, msg

    return True, "✓ NULL-значения в норме"


def check_duplicates(df: pd.DataFrame) -> Tuple[bool, str]:
    """Проверка дубликатов."""
    total_rows = len(df)
    dup_count = df.duplicated().sum()

    if dup_count > 0:
        dup_ratio = (dup_count / total_rows) * 100
        return False, f"⚠ Найдено {dup_count} дубликатов ({dup_ratio:.1f}%)"

    return True, "✓ Дубликатов не найдено"


def check_types(df: pd.DataFrame) -> Tuple[bool, str]:
    """Проверка типов данных."""
    msg = "Типы данных:\n"
    type_count = {}

    for col, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        msg += f"    {col}: {dtype_str}\n"
        type_count[dtype_str] = type_count.get(dtype_str, 0) + 1

    msg += f"\nРаспределение типов:\n"
    for dtype, count in sorted(type_count.items()):
        msg += f"    {dtype}: {count} столбцов\n"

    return True, msg


def check_data_quality(df: pd.DataFrame) -> Tuple[bool, str]:
    """Проверка общего качества данных."""
    msg = "Качество данных:\n"

    total_cells = df.shape[0] * df.shape[1]
    null_cells = df.isnull().sum().sum()
    null_ratio = (null_cells / total_cells) * 100 if total_cells > 0 else 0

    msg += f"    Всего ячеек: {total_cells:,}\n"
    msg += f"    NULL ячеек: {null_cells:,} ({null_ratio:.2f}%)\n"
    msg += f"    Заполненных: {total_cells - null_cells:,} ({100 - null_ratio:.2f}%)\n"

    memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
    msg += f"    Использование памяти: {memory_usage:.2f} МБ\n"

    if null_ratio > 30:
        return False, msg

    return True, msg


def check_numeric_columns(df: pd.DataFrame) -> Tuple[bool, str]:
    """Проверка числовых столбцов на выбросы."""
    numeric_cols = df.select_dtypes(include=['int64', 'int32', 'float64', 'float32']).columns

    if len(numeric_cols) == 0:
        return True, "✓ Числовых столбцов не найдено"

    msg = "Статистика числовых столбцов:\n"

    for col in numeric_cols:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()

        msg += f"    {col}:\n"
        msg += f"      min: {df[col].min():.2f}, max: {df[col].max():.2f}\n"
        msg += f"      mean: {df[col].mean():.2f}, median: {df[col].median():.2f}\n"
        msg += f"      std: {df[col].std():.2f}\n"
        msg += f"      выбросы: {outliers}\n"

    return True, msg


def check_string_columns(df: pd.DataFrame) -> Tuple[bool, str]:
    """Проверка строковых столбцов."""
    string_cols = df.select_dtypes(include=['object', 'string', 'category']).columns

    if len(string_cols) == 0:
        return True, "✓ Строковых столбцов не найдено"

    msg = "Статистика строковых столбцов:\n"

    for col in string_cols:
        unique_count = df[col].nunique()
        null_count = df[col].isnull().sum()

        msg += f"    {col}:\n"
        msg += f"      уникальных значений: {unique_count}\n"
        msg += f"      NULL значений: {null_count}\n"

        if unique_count <= 10:
            msg += f"      значения: {list(df[col].unique())}\n"

    return True, msg


def check_shape(df: pd.DataFrame) -> Tuple[bool, str]:
    """Проверка размера датасета."""
    rows, cols = df.shape
    msg = f"Размер датасета: {rows:,} строк × {cols} столбцов"

    if rows == 0:
        return False, "⚠ " + msg + " (ДАТАСЕТ ПУСТ!)"

    return True, "✓ " + msg


def validate_output(df: pd.DataFrame, verbose: bool = True) -> Dict[str, bool]:
    """
    Полная валидация выходных параметров.
    """
    checks = {
        "shape": check_shape(df),
        "nulls": check_nulls(df),
        "duplicates": check_duplicates(df),
        "types": check_types(df),
        "quality": check_data_quality(df),
        "numeric": check_numeric_columns(df),
        "strings": check_string_columns(df),
    }

    if verbose:
        for check_name, (passed, msg) in checks.items():
            print(msg)
            print()

    all_passed = all(check[0] for check in checks.values())

    results = {check_name: check[0] for check_name, check in checks.items()}
    results['all_passed'] = all_passed

    return results