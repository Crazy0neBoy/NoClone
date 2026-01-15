import argparse
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Generator, Iterable, List

from tqdm import tqdm

# Настройки
DEFAULT_DB = Path("IdsBD.db")
DEFAULT_TXT_DB = Path("IdsBD.txt")  # Для миграции
DEFAULT_INPUT = Path("input.txt")
BATCH_SIZE = 1000  # Размер пачки для SQLite

def setup_logging() -> None:
    """Настройка логирования в папку logs."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Имя лог-файла с датой
    log_filename = log_dir / f"log_{{datetime.now().strftime('%Y-%m-%d')}}.txt"
    
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler()  # Вывод в консоль тоже
        ]
    )

def generate_output_filename() -> Path:
    """Генерация имени выходного файла с текущим временем."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return Path(f"GoodId_{{timestamp}}.txt")

def create_file_if_not_exists(path: Path) -> None:
    if not path.exists():
        path.touch()

def init_db(db_path: Path) -> None:
    """Инициализация базы данных SQLite."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS unique_items (
                content TEXT PRIMARY KEY
            )
            """
        )

def migrate_from_txt_if_needed(txt_path: Path, db_path: Path) -> None:
    if not txt_path.exists():
        return

    logging.info(f"Обнаружена старая база {txt_path}. Начинаем миграцию...")
    
    try:
        lines = txt_path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        lines = txt_path.read_text(encoding="cp1251").splitlines()
    
    unique_items = {line.strip() for line in lines if line.strip()}
    
    if unique_items:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT OR IGNORE INTO unique_items (content) VALUES (?)",
                [(item,) for item in unique_items]
            )
            conn.commit()
            logging.info(f"Мигрировано записей: {len(unique_items)}")

    bak_path = txt_path.with_suffix(".txt.bak")
    txt_path.rename(bak_path)
    logging.info(f"Старый файл переименован в {bak_path}")

def save_words(words: List[str], path: Path) -> None:
    if not words:
        return
    # Если файла нет, создаем. Если есть - дописываем (хотя у нас теперь уникальные имена файлов)
    with path.open("a", encoding="utf-8") as f:
        f.write("\n".join(words) + "\n")

def read_input_file(path: Path) -> List[str]:
    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        content = path.read_text(encoding="cp1251")
    return content.splitlines()

def get_db_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT Count(*) FROM unique_items")
        return cursor.fetchone()[0]

def chunked_iterable(iterable: Iterable, size: int) -> Generator[List, None, None]:
    """Разбивает список на пачки (чанки)."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def process_words_batch(
    input_words: List[str],
    db_path: Path,
    good_id_path: Path,
    show_progress: bool = True,
) -> tuple[int, int, int]:
    """
    Обработка строк пачками (Batch Processing).
    """
    unique_buffer: List[str] = []
    duplicate_count = 0
    skipped_count = 0
    added_count = 0

    # Фильтруем пустые строки сразу
    clean_words = [w.strip() for w in input_words if w.strip()]
    skipped_count = len(input_words) - len(clean_words)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # tqdm теперь идет по чанкам, но total указываем в строках для красоты
        iterator = tqdm(
            chunked_iterable(clean_words, BATCH_SIZE),
            total=(len(clean_words) + BATCH_SIZE - 1) // BATCH_SIZE,
            desc="Обработка (пачками)",
            unit="пачек",
            disable=not show_progress
        )

        for batch in iterator:
            # 1. Убираем дубликаты внутри самой пачки (чтобы не проверять "a", "a" дважды)
            # Но нужно аккуратно считать статистику дублей
            batch_set = set(batch)
            local_dups = len(batch) - len(batch_set)
            duplicate_count += local_dups
            
            if not batch_set:
                continue

            # 2. Проверяем, какие из этих слов УЖЕ есть в базе
            # Генерируем плейсхолдеры (?, ?, ?)
            placeholders = ",".join("?" * len(batch_set))
            query = f"SELECT content FROM unique_items WHERE content IN ({placeholders})"
            
            cursor.execute(query, list(batch_set))
            existing_in_db = {row[0] for row in cursor.fetchall()}
            
            # 3. Вычисляем новые слова
            new_words = [w for w in batch_set if w not in existing_in_db]
            
            # Статистика: те, что нашлись в базе - дубликаты
            duplicate_count += len(existing_in_db)

            # 4. Вставляем новые
            if new_words:
                cursor.executemany(
                    "INSERT INTO unique_items (content) VALUES (?)",
                    [(w,) for w in new_words]
                )
                unique_buffer.extend(new_words)
                added_count += len(new_words)
        
        conn.commit()

    # Сохраняем результат
    if unique_buffer:
        save_words(unique_buffer, good_id_path)

    return added_count, duplicate_count, skipped_count

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NoClone: Удаление дублей с БД SQLite")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Файл базы данных")
    # Если output не передан, он будет None, и мы сгенерируем имя позже
    parser.add_argument("--output", type=Path, default=None, help="Файл результата (по умолчанию GoodId_DATA_TIME.txt)")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Входной файл")
    parser.add_argument("--no-progress", action="store_true", help="Скрыть прогресс-бар")
    return parser.parse_args()

def main() -> None:
    setup_logging()
    args = parse_args()

    # Определяем имя выходного файла
    output_path = args.output if args.output else generate_output_filename()

    logging.info(f"Запуск NoClone. Вход: {args.input}, База: {args.db}")
    
    init_db(args.db)
    if DEFAULT_TXT_DB.exists():
        migrate_from_txt_if_needed(DEFAULT_TXT_DB, args.db)

    if not args.input.exists():
        create_file_if_not_exists(args.input)
        logging.warning(f"Файл {args.input} не найден. Создан пустой файл.")
        print(f"Создан файл: {args.input}. Внесите данные и перезапустите.")
        return

    try:
        inputs = read_input_file(args.input)
    except Exception as e:
        logging.error(f"Ошибка чтения {args.input}: {e}")
        return

    if not inputs and args.input.stat().st_size == 0:
        logging.warning("Входной файл пуст.")
        print("Входной файл пуст.")
        return

    # Обработка
    unique, dups, skipped = process_words_batch(
        inputs,
        args.db,
        output_path,
        show_progress=not args.no_progress,
    )

    # Очистка
    args.input.write_text("", encoding="utf-8")

    total_db = get_db_count(args.db)
    
    # Итоговый отчет в консоль и лог
    stats = (
        f"\n--- Результат ---\n"
        f"Файл результата:    {output_path}\n"
        f"Обработано строк:   {len(inputs) - skipped}\n"
        f"Уникальных (новых): {unique}\n"
        f"Повторов (старых):  {dups}\n"
        f"Всего в базе:       {total_db}"
    )
    print(stats)
    logging.info(stats.replace("\n", " | "))

if __name__ == "__main__":
    main()