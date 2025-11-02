import pandas as pd
import os
from typing import Tuple

def download_from_gdrive(file_id: str) -> pd.DataFrame:
    """
    Загружает CSV-файл с Google Drive по ID.
    """
    url = f"https://drive.google.com/uc?id={file_id}"
    print(f"Загрузка данных из Google Drive (ID: {file_id})...")
    try:
        df = pd.read_csv(url)
        print(f"Успешно загружено: {df.shape[0]} строк, {df.shape[1]} колонок")
        return df
    except Exception as e:
        raise RuntimeError(f"Ошибка загрузки с Google Drive: {e}")

def save_raw(df: pd.DataFrame, path: str) -> None:
    """
    Сохраняет сырые данные в data/raw
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding='utf-8')
    print(f"Сырые данные сохранены: {path}")

def extract(file_id: str = "1cnduXIlbEZTrSB6DfOpV5ifAVzkWkHE9",
            raw_path: str = "data/raw/dataset_raw.csv") -> pd.DataFrame:
    """
    Основная функция extract: загрузка + сохранение raw.
    """
    df = download_from_gdrive(file_id)
    save_raw(df, raw_path)
    return df