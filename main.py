import argparse
from pathlib import Path

from tqdm import tqdm

# Конфигурация по умолчанию
DEFAULT_DB = Path("IdsBD.txt")
DEFAULT_OUTPUT = Path("GoodId.txt")
DEFAULT_INPUT = Path("input.txt")


def create_file_if_not_exists(path: Path) -> None:
    """Создать пустой файл, если его ещё нет."""
    path.touch(exist_ok=True)


def load_database(path: Path) -> set[str]:
    """Загрузить базу уникальных строк в множество, очистив пустые и пробелы."""
    if not path.exists():
        return set()

    lines = path.read_text(encoding="utf-8").splitlines()
    return {line.strip() for line in lines if line.strip()}


def save_words(words: list[str], path: Path) -> None:
    """Сохранить список строк в конец файла, одной пачкой."""
    if not words:
        return

    with path.open("a", encoding="utf-8") as f:
        f.write("\n".join(words) + "\n")


def save_db_atomic(db: set[str], path: Path) -> None:
    """
    Атомарно сохранить содержимое базы в файл.

    Пишем всё множество в временный файл, затем заменяем основной.
    """
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    # Чтобы база была хоть как-то упорядочена — отсортируем
    content = "\n".join(sorted(db))
    tmp_path.write_text(content + ("\n" if content else ""), encoding="utf-8")
    tmp_path.replace(path)


def read_input_file(path: Path) -> list[str]:
    """Прочитать входной файл и очистить его после успешного чтения."""
    lines = path.read_text(encoding="utf-8").splitlines()
    # Очистка будет безопасной, так как к этому моменту данные уже в памяти
    path.write_text("", encoding="utf-8")  # очистка
    return lines


def process_words(
    input_words: list[str],
    db: set[str],
    db_path: Path,
    good_id_path: Path,
    show_progress: bool = True,
) -> tuple[int, int, int, int]:
    """
    Обработать входные строки.

    Возвращает кортеж:
    (кол-во уникальных, повторов, пустых, итоговое количество в базе)
    """
    unique: list[str] = []
    duplicate = 0
    skipped = 0

    iterator = tqdm(input_words, desc="Обработка строк", unit="стр", disable=not show_progress)

    for word in iterator:
        word = word.strip()
        if not word:
            skipped += 1
            continue

        if word in db:
            duplicate += 1
        else:
            db.add(word)
            unique.append(word)

    # Базу сохраняем атомарно (всё множество целиком)
    save_db_atomic(db, db_path)
    # В выходной файл пишем только новые уникальные значения (как раньше)
    save_words(unique, good_id_path)

    return len(unique), duplicate, skipped, len(db)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Удаление повторяющихся строк из файла с ведением базы уникальных значений"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Путь к файлу базы уникальных значений")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Файл для записи новых уникальных строк")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Входной файл со строками")
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Отключить прогресс-бар tqdm (удобно для автоматизации и логов)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Создаём только файлы базы и вывода
    create_file_if_not_exists(args.db)
    create_file_if_not_exists(args.output)

    # Логика создания input.txt
    if not args.input.exists():
        create_file_if_not_exists(args.input)
        print(f"Создан файл: {args.input}")
        print("Внесите в него строки для поиска уникальных и запустите программу снова.")
        return

    # Проверка на пустой файл
    raw_text = args.input.read_text(encoding="utf-8")
    if not raw_text.strip():
        print(f"Файл {args.input} пуст.")
        print("Добавьте строки для поиска уникальных значений и запустите снова.")
        return

    # Если файл существовал и не пуст
    inputs = read_input_file(args.input)

    db = load_database(args.db)

    unique_count, duplicate_count, skipped_count, total_in_db = process_words(
        inputs,
        db,
        args.db,
        args.output,
        show_progress=not args.no_progress,
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