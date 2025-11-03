import pandas as pd
from pathlib import Path
from typing import Union


def validate_source(df: pd.DataFrame) -> bool:
    """Валидация загруженного датасета."""
    if df.empty:
        raise ValueError("Датасет пуст")
    if df.isnull().all().any():
        raise ValueError("Найдены столбцы полностью состоящие из NULL")
    return True


def load_from_google_drive(file_id: str) -> pd.DataFrame:
    """Загрузка файла с Google Drive."""
    file_url = f"https://drive.google.com/uc?id={file_id}"
    try:
        df = pd.read_csv(file_url)
        print(f"✓ Загружено с Google Drive: {df.shape[0]} строк, {df.shape[1]} столбцов")
        return df
    except Exception as e:
        raise ValueError(f"Ошибка загрузки из Google Drive: {e}")


def extract(source_path: Union[str, Path] = None, google_drive_id: str = None) -> pd.DataFrame:
    """
    Загрузка данных из источника.

    Args:
        source_path: Путь к исходному файлу (csv, xlsx, json)
        google_drive_id: ID файла на Google Drive

    Returns:
        pandas.DataFrame с загруженными данными
    """

    if google_drive_id:
        df = load_from_google_drive(google_drive_id)
    elif source_path:
        source_path = Path(source_path)

        if not source_path.exists():
            raise FileNotFoundError(f"Файл не найден: {source_path}")

        if source_path.suffix == '.csv':
            df = pd.read_csv(source_path)
        elif source_path.suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(source_path)
        elif source_path.suffix == '.json':
            df = pd.read_json(source_path)
        else:
            raise ValueError(f"Неподдерживаемый формат: {source_path.suffix}")
    else:
        raise ValueError("Необходимо указать source_path или google_drive_id")

    # Валидация
    validate_source(df)

    # Сохранение сырых данных
    raw_dir = Path('data/raw')
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / 'raw_data.csv'
    df.to_csv(raw_path, index=False)
    print(f"✓ Сырые данные сохранены в {raw_path}")

    return df
