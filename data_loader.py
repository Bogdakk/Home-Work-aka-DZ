import pandas as pd


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

    except Exception as e:
        print(f"Ошибка при загрузке датасета: {e}")


# Запускаем скрипт
if __name__ == "__main__":
    main()