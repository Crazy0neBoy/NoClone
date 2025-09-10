import argparse
from pathlib import Path
from typing import Set

from tqdm import tqdm

# Конфигурация по умолчанию
DEFAULT_DB = Path("IdsBD.txt")
DEFAULT_OUTPUT = Path("GoodId.txt")
DEFAULT_INPUT = Path("input.txt")


def create_file_if_not_exists(path: Path) -> None:
    path.touch(exist_ok=True)


def load_database(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    return set(path.read_text(encoding="utf-8").splitlines())


def save_words(words: list[str], path: Path) -> None:
    if words:  # пишем одним махом
        with path.open("a", encoding="utf-8") as f:
            f.write("\n".join(words) + "\n")


def read_input_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    path.write_text("", encoding="utf-8")  # очистка
    return lines


def process_words(
    input_words: list[str],
    db: Set[str],
    db_path: Path,
    good_id_path: Path,
) -> tuple[int, int, int, int]:
    unique, duplicate, skipped = [], 0, 0

    for word in tqdm(input_words, desc="Обработка строк", unit="стр"):
        word = word.strip()
        if not word:
            skipped += 1
            continue
        if word in db:
            duplicate += 1
        else:
            db.add(word)
            unique.append(word)

    # сохраняем только новые строки
    save_words(unique, db_path)
    save_words(unique, good_id_path)

    return len(unique), duplicate, skipped, len(db)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Удаление повторяющихся строк из текстового файла"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Путь к файлу базы уникальных строк",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Путь к файлу для новых уникальных строк",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Путь к входному файлу",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Подготовка файлов
    for file in (args.db, args.output, args.input):
        create_file_if_not_exists(file)

    db = load_database(args.db)
    inputs = read_input_file(args.input)

    unique_count, duplicate_count, skipped_count, total_in_db = process_words(
        inputs, db, args.db, args.output
    )

    processed_count = len(inputs) - skipped_count

    print("\n--- Статистика ---")
    print(f"Всего строк прочитано:  {len(inputs)}")
    print(f"Пропущено пустых:       {skipped_count}")
    print(f"Всего строк обработано: {processed_count}")
    print(f"Уникальных добавлено:   {unique_count}")
    print(f"Повторов найдено:       {duplicate_count}")
    print(f"Всего строк в базе:     {total_in_db}")


if __name__ == "__main__":
    main()