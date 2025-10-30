import requests
import pandas as pd
from datetime import datetime
import os


class FoxAPIReader:
    """Класс для работы с Random Fox API"""

    def __init__(self):
        self.base_url = "https://randomfox.ca/floof/"
        self.data = None

    def fetch_fox_data(self):
        """Получение данных о случайной лисе из API"""
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()  # Проверка на ошибки HTTP

            self.data = response.json()
            print(" Данные успешно получены!")
            return self.data

        except requests.exceptions.RequestException as e:
            print(f" Ошибка при запросе к API: {e}")
            return None

    def to_dataframe(self):
        """Преобразование данных в Pandas DataFrame"""
        if self.data is None:
            print(" Нет данных для преобразования. Сначала выполните fetch_fox_data()")
            return None

        try:
            # Создаем DataFrame из полученных данных
            df_data = {
                'image_url': [self.data.get('image', '')],
                'link': [self.data.get('link', '')],
                'timestamp': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            }

            df = pd.DataFrame(df_data)
            print(" Данные успешно преобразованы в DataFrame!")
            return df

        except Exception as e:
            print(f" Ошибка при создании DataFrame: {e}")
            return None

    def display_info(self):
        """Отображение информации о полученных данных"""
        if self.data is None:
            print(" Нет данных для отображения")
            return

        print("\n" + "=" * 50)
        print(" ИНФОРМАЦИЯ О СЛУЧАЙНОЙ ЛИСЕ")
        print("=" * 50)
        print(f"🖼 URL изображения: {self.data.get('image', 'Не указан')}")
        print(f" Ссылка: {self.data.get('link', 'Не указана')}")
        print("=" * 50)


def main():
    """Основная функция для демонстрации работы с API"""
    print(" Запуск Fox API Reader...")

    # Создаем экземпляр класса
    fox_reader = FoxAPIReader()

    # Получаем данные из API
    fox_data = fox_reader.fetch_fox_data()

    if fox_data:
        # Отображаем информацию
        fox_reader.display_info()

        # Преобразуем в DataFrame
        df = fox_reader.to_dataframe()

        if df is not None:
            # Выводим DataFrame
            print("\n Pandas DataFrame:")
            print(df)

            # Дополнительная информация о DataFrame
            print(f"\n Информация о DataFrame:")
            print(f"   Количество строк: {len(df)}")
            print(f"   Количество столбцов: {len(df.columns)}")
            print(f"   Столбцы: {list(df.columns)}")

            # Сохраняем в CSV (опционально)
            csv_filename = f"fox_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_filename, index=False)
            print(f" Данные сохранены в файл: {csv_filename}")

    print("\n Программа завершена!")


if __name__ == "__main__":
    main()