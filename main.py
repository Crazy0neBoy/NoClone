# ---===== АнтиДубликат =====---
from pathlib import Path
from tqdm import tqdm
from typing import Set

# Конфигурация
AD_DATABASE = Path("IdsBD.txt")
GOOD_ID_FILE = Path("GoodId.txt")
INPUT_FILE = Path("input.txt")


def create_file_if_not_exists(path: Path) -> None:
    path.touch(exist_ok=True)


def load_database(path: Path = AD_DATABASE) -> Set[str]:
    if not path.exists():
        return set()
    return set(path.read_text(encoding="utf-8").splitlines())


def save_words(words: list[str], path: Path) -> None:
    if words:  # пишем одним махом
        with path.open("a", encoding="utf-8") as f:
            f.write("\n".join(words) + "\n")


def read_input_file(path: Path = INPUT_FILE) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    path.write_text("", encoding="utf-8")  # очистка
    return lines


def process_words(input_words: list[str], db: Set[str]) -> tuple[int, int, int]:
    unique, duplicate = [], 0

    for word in tqdm(input_words, desc="Обработка строк", unit="стр"):
        if word in db:
            duplicate += 1
        else:
            db.add(word)
            unique.append(word)

    # сохраняем только новые строки
    save_words(unique, AD_DATABASE)
    save_words(unique, GOOD_ID_FILE)

    return len(unique), duplicate, len(db)


def main():
    # Подготовка файлов
    for file in (AD_DATABASE, GOOD_ID_FILE, INPUT_FILE):
        create_file_if_not_exists(file)

    db = load_database()
    inputs = read_input_file()

    unique_count, duplicate_count, total_in_db = process_words(inputs, db)

    print("\n--- Статистика ---")
    print(f"Всего строк обработано: {len(inputs)}")
    print(f"Уникальных добавлено:   {unique_count}")
    print(f"Повторов найдено:      {duplicate_count}")
    print(f"Всего строк в базе:    {total_in_db}")


if __name__ == "__main__":
    main()
# ---===== АнтиДубликат =====---
