import pandas as pd
import numpy as np


def main():
    print("Загрузка датасета из Google Drive...")

    # ID файла из вашей ссылки
    FILE_ID = "1cnduXIlbEZTrSB6DfOpV5ifAVzkWkHE9"
    file_url = f"https://drive.google.com/uc?id={FILE_ID}"

    try:
        # Читаем файл
        raw_data = pd.read_csv(file_url)
        print("Датасет успешно загружен!")

        # Выводим первые 10 строк
        print("\n" + "=" * 60)
        print("ПЕРВЫЕ 10 СТРОК ДАТАСЕТА:")
        print("=" * 60)
        print(raw_data.head(10))

        # Выводим информацию о типах данных перед преобразованием
        print("\n" + "=" * 60)
        print("ИНФОРМАЦИЯ О ДАТАСЕТЕ ДО ПРЕОБРАЗОВАНИЯ:")
        print("=" * 60)
        print(raw_data.info())
        print("\nТипы данных до преобразования:")
        print(raw_data.dtypes)

        # Приведение типов для DataFrame
        df_cleaned = convert_data_types(raw_data)

        # Выводим информацию о типах данных после преобразования
        print("\n" + "=" * 60)
        print("ИНФОРМАЦИЯ О ДАТАСЕТЕ ПОСЛЕ ПРЕОБРАЗОВАНИЯ:")
        print("=" * 60)
        print(df_cleaned.info())
        print("\nТипы данных после преобразования:")
        print(df_cleaned.dtypes)

        # Сохраняем в подходящий формат
        save_dataset(df_cleaned)

    except Exception as e:
        print(f"Ошибка при загрузке датасета: {e}")


def convert_data_types(df):
    """
    Функция для приведения типов данных в DataFrame
    """
    print("\n" + "=" * 60)
    print("ПРИВЕДЕНИЕ ТИПОВ ДАННЫХ:")
    print("=" * 60)

    # Создаем копию DataFrame для преобразований
    df_converted = df.copy()

    # Проходим по всем колонкам и преобразуем типы
    for column in df_converted.columns:
        original_dtype = df_converted[column].dtype
        unique_count = df_converted[column].nunique()

        print(f"\nКолонка: {column}")
        print(f"  Исходный тип: {original_dtype}")
        print(f"  Уникальных значений: {unique_count}")
        print(f"  Примеры значений: {df_converted[column].head(3).tolist()}")

        # Преобразование в категориальный тип для колонок с небольшим количеством уникальных значений
        if unique_count <= 20 and df_converted[column].dtype == 'object':
            df_converted[column] = df_converted[column].astype('category')
            print(f"  Преобразовано в: category")

        # Преобразование строковых числовых значений в числа
        elif df_converted[column].dtype == 'object':
            # Пробуем преобразовать в числовой тип
            converted = pd.to_numeric(df_converted[column], errors='coerce')
            if not converted.isna().all():  # Если удалось преобразовать хотя бы некоторые значения
                df_converted[column] = converted
                print(f"  Преобразовано в: numeric")
            else:
                # Оставляем как строку, но оптимизируем память
                df_converted[column] = df_converted[column].astype('string')
                print(f"  Преобразовано в: string (оптимизировано)")

        # Преобразование целых чисел в оптимальный тип
        elif pd.api.types.is_integer_dtype(df_converted[column]):
            df_converted[column] = pd.to_numeric(df_converted[column], downcast='integer')
            print(f"  Преобразовано в: {df_converted[column].dtype}")

        # Преобразование чисел с плавающей точкой в оптимальный тип
        elif pd.api.types.is_float_dtype(df_converted[column]):
            df_converted[column] = pd.to_numeric(df_converted[column], downcast='float')
            print(f"  Преобразовано в: {df_converted[column].dtype}")

        # Преобразование boolean
        elif df_converted[column].dtype == 'bool':
            df_converted[column] = df_converted[column].astype('boolean')
            print(f"  Преобразовано в: boolean")

        else:
            print(f"  Тип оставлен без изменений: {df_converted[column].dtype}")

    # Дополнительная обработка дат, если есть колонки с подозрением на даты
    date_columns = detect_date_columns(df_converted)
    for col in date_columns:
        print(f"\nОбнаружена колонка с датами: {col}")
        df_converted[col] = pd.to_datetime(df_converted[col], errors='coerce')
        print(f"  Преобразовано в: datetime64")

    return df_converted


def detect_date_columns(df):
    """
    Функция для автоматического обнаружения колонок с датами
    """
    date_columns = []
    date_keywords = ['date', 'time', 'day', 'month', 'year', 'created', 'updated', 'timestamp']

    for column in df.columns:
        col_lower = column.lower()
        # Проверяем по ключевым словам в названии колонки
        if any(keyword in col_lower for keyword in date_keywords):
            date_columns.append(column)
        # Проверяем по содержимому (первые несколько ненулевых значений)
        elif df[column].dtype == 'object':
            sample = df[column].dropna().head(5)
            if not sample.empty and sample.astype(str).str.match(r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}').any():
                date_columns.append(column)

    return date_columns


def save_dataset(df):
    """
    Функция для сохранения DataFrame в подходящий формат
    """
    print("\n" + "=" * 60)
    print("СОХРАНЕНИЕ ДАТАСЕТА:")
    print("=" * 60)

    # Определяем оптимальный формат на основе размера данных и типов
    num_rows, num_cols = df.shape
    memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024  # в МБ

    print(f"Размер датасета: {num_rows} строк × {num_cols} колонок")
    print(f"Память: {memory_usage:.2f} МБ")

    # Сохраняем в нескольких форматах
    formats = []

    # 1. Parquet (рекомендуется для больших датасетов)
    if num_rows > 10000 or memory_usage > 50:
        parquet_file = "dataset_cleaned.parquet"
        df.to_parquet(parquet_file, index=False)
        formats.append(("Parquet", parquet_file))
        print(f"✓ Сохранено в Parquet: {parquet_file}")

    # 2. Feather (быстрая загрузка)
    feather_file = "dataset_cleaned.feather"
    df.to_feather(feather_file)
    formats.append(("Feather", feather_file))
    print(f"✓ Сохранено в Feather: {feather_file}")

    # 3. CSV (универсальный формат)
    csv_file = "dataset_cleaned.csv"
    df.to_csv(csv_file, index=False, encoding='utf-8')
    formats.append(("CSV", csv_file))
    print(f"✓ Сохранено в CSV: {csv_file}")

    # 4. Pickle (сохраняет все типы данных)
    pickle_file = "dataset_cleaned.pkl"
    df.to_pickle(pickle_file)
    formats.append(("Pickle", pickle_file))
    print(f"✓ Сохранено в Pickle: {pickle_file}")

    print(f"\nИтог: датасет сохранен в {len(formats)} форматах:")
    for format_name, filename in formats:
        print(f"  - {format_name}: {filename}")

    # Рекомендуемый формат для дальнейшего использования
    recommended_format = "Feather" if num_rows <= 100000 else "Parquet"
    print(f"\nРекомендуемый формат для работы: {recommended_format}")


# Запускаем скрипт
if __name__ == "__main__":
    main()