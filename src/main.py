import argparse
import sys
from time import time

# Импортируем основные функции из ETL-модулей.
# Скрипт упадёт с ошибкой, если нужные файлы не найдены.
try:
    from extract import extract
    from transform import transform
    from load import load
except ImportError as e:
    print(f"Ошибка импорта: {e}.")
    print("Убедитесь, что файлы extract.py, transform.py и load.py находятся в той же директории, что и main.py.")
    sys.exit(1)


def run_extract():
    """Обертка для запуска и логирования этапа Extract."""
    print("="*50)
    print("НАЧАЛО ЭТАПА: EXTRACT")
    print("="*50)
    start_time = time()
    extract()
    end_time = time()
    print(f"ЭТАП EXTRACT ЗАВЕРШЕН. Время выполнения: {end_time - start_time:.2f} сек.")
    print("="*50 + "\n")


def run_transform():
    """Обертка для запуска и логирования этапа Transform."""
    print("="*50)
    print("НАЧАЛО ЭТАПА: TRANSFORM")
    print("="*50)
    start_time = time()
    transform()
    end_time = time()
    print(f"ЭТАП TRANSFORM ЗАВЕРШЕН. Время выполнения: {end_time - start_time:.2f} сек.")
    print("="*50 + "\n")


def run_load():
    """Обертка для запуска и логирования этапа Load."""
    print("="*50)
    print("НАЧАЛО ЭТАПА: LOAD")
    print("="*50)
    start_time = time()
    load()
    end_time = time()
    print(f"ЭТАП LOAD ЗАВЕРШЕН. Время выполнения: {end_time - start_time:.2f} сек.")
    print("="*50 + "\n")


def run_all():
    """Последовательно запускает все этапы ETL-пайплайна."""
    start_total_time = time()
    run_extract()
    run_transform()
    run_load()
    end_total_time = time()
    print("="*50)
    print("ВЕСЬ ETL-ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!")
    print(f"Общее время выполнения: {end_total_time - start_total_time:.2f} сек.")
    print("="*50)


def main():
    """
    Основная функция для парсинга аргументов командной строки
    и запуска соответствующих ETL-процессов.
    """
    parser = argparse.ArgumentParser(
        description="ETL-пайплайн для обработки данных от извлечения до загрузки.",
        epilog="Пример использования: python main.py all"
    )

    # `dest='command'` сохранит имя подкоманды (all, extract, и т.д.)
    # `required=True` делает выбор одной из подкоманд обязательным
    subparsers = parser.add_subparsers(
        dest='command',
        required=True,
        help="Доступные команды"
    )

    # Команда для запуска всего пайплайна
    subparsers.add_parser(
        'all',
        help="Запустить весь ETL-пайплайн (Extract -> Transform -> Load)."
    )

    # Команда для запуска только этапа Extract
    subparsers.add_parser(
        'extract',
        help="Запустить только этап извлечения данных (Extract)."
    )

    # Команда для запуска только этапа Transform
    subparsers.add_parser(
        'transform',
        help="Запустить только этап трансформации данных (Transform)."
    )

    # Команда для запуска только этапа Load
    subparsers.add_parser(
        'load',
        help="Запустить только этап загрузки данных (Load)."
    )

    args = parser.parse_args()

    try:
        # Выполняем функцию в зависимости от выбранной команды
        if args.command == "all":
            run_all()
        elif args.command == "extract":
            run_extract()
        elif args.command == "transform":
            run_transform()
        elif args.command == "load":
            run_load()
    except Exception as e:
        print(f"\nКРИТИЧЕСКАЯ ОШИБКА в процессе выполнения '{args.command}':")
        print(f"-> {e}")
        sys.exit(1)


# === ТОЧКА ВХОДА ===
if __name__ == "__main__":
    main()