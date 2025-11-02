import pandas as pd
import os
from pathlib import Path
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


def save_raw(df: pd.DataFrame, relative_path: str) -> None:
    """
    Сохраняет сырые данные в указанный путь относительно расположения скрипта.
    """
    # Путь относительно директории скрипта
    script_dir = Path(__file__).parent.resolve()
    full_path = script_dir / relative_path

    try:
        # Создаём папки, если нужно
        os.makedirs(full_path.parent, exist_ok=True)
        df.to_csv(full_path, index=False, encoding='utf-8')
        print(f"Сырые данные сохранены: {full_path}")
    except PermissionError:
        raise RuntimeError(f"Нет прав на запись в {full_path}")
    except Exception as e:
        raise RuntimeError(f"Ошибка при сохранении {full_path}: {e}")


def extract(
    file_id: str = "1cnduXIlbEZTrSB6DfOpV5ifAVzkWkHE9",
    raw_path: str = "data/raw/dataset_raw.csv"
) -> pd.DataFrame:
    """
    Основная функция extract: загрузка + сохранение raw.
    """
    df = download_from_gdrive(file_id)
    save_raw(df, raw_path)
    return df


# === ТОЧКА ВХОДА: для запуска напрямую ===
if __name__ == "__main__":
    try:
        df = extract()
        print("\nПервые 2 строки данных:")
        print(df.head(2))
        print(f"\nДанные успешно извлечены и сохранены в: {Path('data/raw/dataset_raw.csv').resolve()}")
    except Exception as e:
        print(f"Ошибка: {e}")