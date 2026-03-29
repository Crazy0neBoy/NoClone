import sys
import logging
import hashlib
import sqlite3
import re
from pathlib import Path
from datetime import datetime
from typing import Generator, Iterable, List, Optional, Callable

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QFileDialog, QProgressBar,
    QTextEdit, QMessageBox, QTabWidget, QSplitter, QGroupBox,
    QCheckBox, QSpinBox
)
from PySide6.QtCore import Qt, QThread, Signal, Slot, QSettings
from PySide6.QtGui import QGuiApplication, QDragEnterEvent, QDropEvent

# --- Настройки и логика (Core) ---

DEFAULT_DB = Path("IdsBD.db")
DEFAULT_TXT_DB = Path("IdsBD.txt")
DEFAULT_INPUT = Path("input.txt")
BATCH_SIZE = 5000  # Увеличили размер пачки для скорости

def setup_logging() -> None:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_filename = log_dir / f"log_{datetime.now().strftime('%Y-%m-%d')}.txt"
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

def generate_output_filename() -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return Path(f"GoodId_{timestamp}.txt")

def init_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        # ОПТИМИЗАЦИЯ: Ускорение записи SQLite
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("CREATE TABLE IF NOT EXISTS unique_items (content TEXT PRIMARY KEY)")

def vacuum_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("VACUUM")

def get_db_stats(db_path: Path) -> tuple[int, float]:
    """Возвращает (кол-во записей, размер в МБ)"""
    count = 0
    size_mb = 0.0
    if Path(db_path).exists():
        size_mb = Path(db_path).stat().st_size / (1024 * 1024)
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute("SELECT Count(*) FROM unique_items")
                count = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            pass # База еще не создана
    return count, size_mb

def migrate_from_txt_if_needed(txt_path: Path, db_path: Path) -> None:
    if not txt_path.exists(): return
    try:
        lines = txt_path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        lines = txt_path.read_text(encoding="cp1251").splitlines()
    
    unique_items = {line.strip() for line in lines if line.strip()}
    if unique_items:
        with sqlite3.connect(db_path) as conn:
            conn.executemany("INSERT OR IGNORE INTO unique_items (content) VALUES (?)", [(item,) for item in unique_items])
    
    bak_path = txt_path.with_suffix(".txt.bak")
    txt_path.rename(bak_path)

def save_words(words: List[str], path: Path) -> None:
    if not words: return
    with path.open("a", encoding="utf-8") as f:
        f.write("\n".join(words) + "\n")

def chunked_iterable(iterable: Iterable, size: int) -> Generator[List, None, None]:
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk: yield chunk

def process_words_batch(
    line_generator: Iterable[str],
    db_path: Path,
    output_path: Optional[Path] = None,
    lowercase: bool = False,
    remove_special: bool = False,
    min_length: int = 0,
    progress_callback: Optional[Callable[[int], None]] = None,
) -> tuple[List[str], int, int, int]:
    
    unique_buffer: List[str] = []
    duplicate_count = 0
    skipped_count = 0
    added_count = 0

    pattern = re.compile(r'[^a-zA-Z0-9а-яА-Я]') if remove_special else None

    # ОПТИМИЗАЦИЯ: Генератор для очистки на лету
    def clean_generator():
        nonlocal skipped_count
        for line in line_generator:
            w = line.strip()
            if not w:
                skipped_count += 1
                continue
            if lowercase:
                w = w.lower()
            if pattern:
                w = pattern.sub('', w)
            if len(w) < min_length:
                skipped_count += 1
                continue
            if not w:
                skipped_count += 1
                continue
            yield w

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        cursor = conn.cursor()
        
        for i, batch in enumerate(chunked_iterable(clean_generator(), BATCH_SIZE)):
            if progress_callback: progress_callback(i + 1)
            
            batch_set = set(batch)
            duplicate_count += (len(batch) - len(batch_set))
            
            if not batch_set: continue
            
            placeholders = ",".join("?" * len(batch_set))
            cursor.execute(f"SELECT content FROM unique_items WHERE content IN ({placeholders})", list(batch_set))
            existing_in_db = {row[0] for row in cursor.fetchall()}
            
            new_words = [w for w in batch_set if w not in existing_in_db]
            duplicate_count += len(existing_in_db)

            if new_words:
                cursor.executemany("INSERT INTO unique_items (content) VALUES (?)", [(w,) for w in new_words])
                unique_buffer.extend(new_words)
                added_count += len(new_words)
        conn.commit()

    if output_path and unique_buffer:
        save_words(unique_buffer, output_path)

    return unique_buffer, added_count, duplicate_count, skipped_count

# --- GUI Components ---

class LogHandler(logging.Handler):
    def __init__(self, signal):
        super().__init__()
        self.signal = signal
    def emit(self, record):
        self.signal.emit(self.format(record))

class Worker(QThread):
    progress = Signal(int)
    max_progress = Signal(int)
    log_signal = Signal(str)
    finished_signal = Signal(dict)
    error_signal = Signal(str)

    def __init__(self, input_data, db_path, output_path=None, is_manual=False, options=None):
        super().__init__()
        self.input_data = input_data
        self.db_path = Path(db_path)
        self.output_path = Path(output_path) if output_path else None
        self.is_manual = is_manual
        self.options = options or {}

    def run(self):
        try:
            init_db(self.db_path)
            if DEFAULT_TXT_DB.exists():
                migrate_from_txt_if_needed(DEFAULT_TXT_DB, self.db_path)

            if self.is_manual:
                lines = self.input_data
                total_lines = len(lines)
                def line_gen():
                    for line in lines: yield line
                generator = line_gen()
            else:
                input_path = Path(self.input_data)
                if not input_path.exists():
                    self.error_signal.emit(f"Файл {input_path} не найден.")
                    return
                
                # ОПТИМИЗАЦИЯ: Быстрый подсчет строк для прогресс-бара без загрузки файла в память
                self.log_signal.emit("Подсчет строк...")
                total_lines = 0
                with open(input_path, 'rb') as f:
                    total_lines = sum(1 for _ in f)
                
                def line_gen():
                    with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
                        for line in f: yield line
                generator = line_gen()

            if total_lines == 0:
                self.finished_signal.emit({"status": "empty"})
                return

            total_batches = (total_lines + BATCH_SIZE - 1) // BATCH_SIZE
            self.max_progress.emit(total_batches)

            unique_list, unique_count, dups, skipped = process_words_batch(
                line_generator=generator,
                db_path=self.db_path,
                output_path=self.output_path,
                lowercase=self.options.get('lowercase', False),
                remove_special=self.options.get('remove_special', False),
                min_length=self.options.get('min_length', 0),
                progress_callback=self.progress.emit
            )

            if not self.is_manual:
                Path(self.input_data).write_text("", encoding="utf-8")

            count, size = get_db_stats(self.db_path)
            self.finished_signal.emit({
                "status": "success", "unique_list": unique_list,
                "unique_count": unique_count, "dups": dups,
                "total_db": count, "db_size": size,
                "output_file": str(self.output_path) if self.output_path else None
            })
        except Exception as e:
            logging.exception("Ошибка в рабочем потоке")
            self.error_signal.emit(str(e))

class VacuumWorker(QThread):
    finished_signal = Signal(dict)
    error_signal = Signal(str)

    def __init__(self, db_path):
        super().__init__()
        self.db_path = Path(db_path)

    def run(self):
        try:
            old_count, old_size = get_db_stats(self.db_path)
            vacuum_db(self.db_path)
            new_count, new_size = get_db_stats(self.db_path)
            self.finished_signal.emit({
                "old_size": old_size, "new_size": new_size
            })
        except Exception as e:
            self.error_signal.emit(str(e))

class NoCloneGUI(QMainWindow):
    log_added = Signal(str)

    def __init__(self):
        super().__init__()
        setup_logging()
        self.setWindowTitle("NoClone GUI - Pro Edition")
        self.setMinimumSize(900, 700)
        self.setAcceptDrops(True) # Включаем Drag & Drop
        
        self.settings = QSettings("MyCompany", "NoClone")
        self.last_manual_hash = None
        
        self.setup_ui()
        self.load_settings()
        self.setup_logging_bridge()
        self.update_stats_ui()

    def setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # --- Статистика ---
        stats_layout = QHBoxLayout()
        self.lbl_stats = QLabel("База данных: 0 записей (0.00 МБ)")
        self.lbl_stats.setStyleSheet("font-weight: bold; color: #333;")
        stats_layout.addWidget(self.lbl_stats)
        
        self.btn_vacuum = QPushButton("Оптимизировать БД (Сжать)")
        self.btn_vacuum.setToolTip("Очищает пустое место на диске после удаления данных")
        self.btn_vacuum.clicked.connect(self.start_vacuum)
        stats_layout.addWidget(self.btn_vacuum, alignment=Qt.AlignRight)
        main_layout.addLayout(stats_layout)

        # --- Настройки обработки ---
        options_group = QGroupBox("Правила обработки строк")
        options_layout = QHBoxLayout()
        
        self.chk_lower = QCheckBox("К нижнему регистру")
        self.chk_special = QCheckBox("Удалить спецсимволы (оставить буквы/цифры)")
        
        len_layout = QHBoxLayout()
        len_layout.addWidget(QLabel("Мин. длина строки:"))
        self.spin_len = QSpinBox()
        self.spin_len.setMinimum(0)
        self.spin_len.setMaximum(100)
        len_layout.addWidget(self.spin_len)
        
        options_layout.addWidget(self.chk_lower)
        options_layout.addWidget(self.chk_special)
        options_layout.addStretch()
        options_layout.addLayout(len_layout)
        options_group.setLayout(options_layout)
        main_layout.addWidget(options_group)

        # --- Вкладки ---
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # 1. Ручной режим
        manual_tab = QWidget()
        manual_layout = QVBoxLayout(manual_tab)
        splitter = QSplitter(Qt.Horizontal)
        
        input_container = QWidget()
        input_vbox = QVBoxLayout(input_container)
        input_vbox.addWidget(QLabel("Вставьте строки сюда:"))
        self.manual_input = QTextEdit()
        self.manual_input.setPlaceholderText("Один ID на строку...")
        input_vbox.addWidget(self.manual_input)
        splitter.addWidget(input_container)
        
        output_container = QWidget()
        output_vbox = QVBoxLayout(output_container)
        output_vbox.addWidget(QLabel("Результат (новые уникальные):"))
        self.manual_output = QTextEdit()
        self.manual_output.setReadOnly(True)
        output_vbox.addWidget(self.manual_output)
        
        self.copy_btn = QPushButton("Копировать результат")
        self.copy_btn.clicked.connect(self.copy_to_clipboard)
        output_vbox.addWidget(self.copy_btn)
        splitter.addWidget(output_container)
        
        manual_layout.addWidget(splitter)
        self.manual_start_btn = QPushButton("Очистить дубликаты и добавить в базу")
        self.manual_start_btn.setFixedHeight(45)
        self.manual_start_btn.setStyleSheet("font-weight: bold; font-size: 14px;")
        self.manual_start_btn.clicked.connect(self.start_manual_processing)
        manual_layout.addWidget(self.manual_start_btn)
        self.tabs.addTab(manual_tab, "Ручной режим (Paste & Go)")

        # 2. Файловый режим
        file_tab = QWidget()
        file_layout = QVBoxLayout(file_tab)
        self.input_edit = self.create_file_selector(file_layout, "Входной файл (*.txt):", str(DEFAULT_INPUT))
        self.db_edit = self.create_file_selector(file_layout, "База данных (*.db):", str(DEFAULT_DB))
        self.output_edit = self.create_file_selector(file_layout, "Файл результата (опционально):", "", is_save=True)
        
        lbl_drag = QLabel("(Можно перетащить файл .txt прямо в окно программы)")
        lbl_drag.setStyleSheet("color: gray; font-style: italic;")
        file_layout.addWidget(lbl_drag)

        self.file_start_btn = QPushButton("Начать обработку файла")
        self.file_start_btn.setFixedHeight(40)
        self.file_start_btn.clicked.connect(self.start_file_processing)
        file_layout.addWidget(self.file_start_btn)
        file_layout.addStretch()
        self.tabs.addTab(file_tab, "Файловый режим")

        # --- Прогресс и логи ---
        self.progress_bar = QProgressBar()
        self.progress_bar.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.progress_bar)

        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumHeight(120)
        main_layout.addWidget(QLabel("Логи событий:"))
        main_layout.addWidget(self.log_view)

    def create_file_selector(self, parent_layout, label_text, default_val, is_save=False):
        h_layout = QHBoxLayout()
        h_layout.addWidget(QLabel(label_text))
        line_edit = QLineEdit(default_val)
        h_layout.addWidget(line_edit)
        btn = QPushButton("Обзор...")
        btn.clicked.connect(lambda: self.browse_file(line_edit, is_save))
        h_layout.addWidget(btn)
        parent_layout.addLayout(h_layout)
        return line_edit

    def browse_file(self, line_edit, is_save):
        if is_save:
            path, _ = QFileDialog.getSaveFileName(self, "Выбрать файл", line_edit.text(), "Text (*.txt)")
        else:
            path, _ = QFileDialog.getOpenFileName(self, "Выбрать файл", line_edit.text(), "Text/DB (*.txt *.db)")
        if path: line_edit.setText(path)

    def load_settings(self):
        self.db_edit.setText(self.settings.value("db_path", str(DEFAULT_DB)))
        self.input_edit.setText(self.settings.value("input_path", str(DEFAULT_INPUT)))
        
        # Загрузка чекбоксов (QSettings возвращает строки 'true'/'false', преобразуем аккуратно)
        self.chk_lower.setChecked(self.settings.value("chk_lower", False, type=bool))
        self.chk_special.setChecked(self.settings.value("chk_special", False, type=bool))
        self.spin_len.setValue(self.settings.value("spin_len", 0, type=int))

    def closeEvent(self, event):
        # Сохраняем настройки при закрытии
        self.settings.setValue("db_path", self.db_edit.text())
        self.settings.setValue("input_path", self.input_edit.text())
        self.settings.setValue("chk_lower", self.chk_lower.isChecked())
        self.settings.setValue("chk_special", self.chk_special.isChecked())
        self.settings.setValue("spin_len", self.spin_len.value())
        super().closeEvent(event)

    # --- Drag & Drop ---
    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event: QDropEvent):
        urls = event.mimeData().urls()
        if urls:
            path = urls[0].toLocalFile()
            if path.endswith('.txt'):
                self.input_edit.setText(path)
                self.tabs.setCurrentIndex(1) # Переключаем на вкладку файлов
                logging.info(f"Файл загружен через Drag&Drop: {path}")
            elif path.endswith('.db'):
                self.db_edit.setText(path)
                self.update_stats_ui()
                logging.info(f"База загружена через Drag&Drop: {path}")

    def setup_logging_bridge(self):
        logger = logging.getLogger()
        handler = LogHandler(self.log_added)
        formatter = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.log_added.connect(lambda text: self.log_view.append(text))

    def update_stats_ui(self):
        db_path = Path(self.db_edit.text())
        count, size = get_db_stats(db_path)
        self.lbl_stats.setText(f"Текущая база: {count} записей ({size:.2f} МБ)")

    def get_options(self):
        return {
            'lowercase': self.chk_lower.isChecked(),
            'remove_special': self.chk_special.isChecked(),
            'min_length': self.spin_len.value()
        }

    def start_manual_processing(self):
        text = self.manual_input.toPlainText().strip()
        if not text: return
        
        # Учитываем настройки в хэше, чтобы при изменении галочек можно было запустить снова
        opts = str(self.get_options())
        current_hash = hashlib.md5((text + opts).encode('utf-8')).hexdigest()
        
        if current_hash == self.last_manual_hash:
            QMessageBox.information(self, "Инфо", "Эти данные с текущими настройками уже обработаны.")
            return
            
        self.manual_output.clear()
        self.last_manual_hash = current_hash
        self.run_worker(text.splitlines(), self.db_edit.text(), is_manual=True)

    def start_file_processing(self):
        self.run_worker(self.input_edit.text(), self.db_edit.text(), self.output_edit.text() or generate_output_filename())

    def run_worker(self, input_data, db_path, output_path=None, is_manual=False):
        self.file_start_btn.setEnabled(False)
        self.manual_start_btn.setEnabled(False)
        self.btn_vacuum.setEnabled(False)
        self.progress_bar.setValue(0)
        
        self.worker = Worker(input_data, db_path, output_path, is_manual, self.get_options())
        self.worker.progress.connect(self.progress_bar.setValue)
        self.worker.max_progress.connect(self.progress_bar.setMaximum)
        self.worker.finished_signal.connect(self.handle_finished)
        self.worker.error_signal.connect(self.handle_error)
        self.worker.start()

    def start_vacuum(self):
        db_path = self.db_edit.text()
        if not Path(db_path).exists():
            QMessageBox.warning(self, "Ошибка", "База данных еще не создана.")
            return
            
        self.btn_vacuum.setEnabled(False)
        self.file_start_btn.setEnabled(False)
        self.manual_start_btn.setEnabled(False)
        logging.info("Запуск оптимизации базы данных...")
        
        self.vac_worker = VacuumWorker(db_path)
        self.vac_worker.finished_signal.connect(self.handle_vacuum_finished)
        self.vac_worker.error_signal.connect(self.handle_error)
        self.vac_worker.start()

    @Slot(str)
    def handle_error(self, m):
        QMessageBox.critical(self, "Ошибка", m)
        self.restore_ui_state()
        self.last_manual_hash = None

    @Slot(dict)
    def handle_vacuum_finished(self, res):
        self.restore_ui_state()
        self.update_stats_ui()
        saved = res['old_size'] - res['new_size']
        logging.info(f"Оптимизация завершена. Освобождено {saved:.2f} МБ.")
        QMessageBox.information(self, "Готово", f"База оптимизирована!\nОсвобождено: {saved:.2f} МБ")

    @Slot(dict)
    def handle_finished(self, res):
        self.restore_ui_state()
        if res["status"] == "success":
            if res.get("unique_list") is not None:
                self.manual_output.setPlainText("\n".join(res["unique_list"]))
            
            # Обновляем UI со статистикой
            self.lbl_stats.setText(f"Текущая база: {res['total_db']} записей ({res['db_size']:.2f} МБ)")
            
            msg = f"Готово!\nНовых: {res['unique_count']}\nДублей: {res['dups']}\nВ базе: {res['total_db']}"
            if res.get("output_file"):
                msg += f"\nФайл: {res['output_file']}"
            QMessageBox.information(self, "Завершено", msg)
        else:
            self.last_manual_hash = None
            QMessageBox.warning(self, "Пусто", "Нет данных.")

    def restore_ui_state(self):
        self.file_start_btn.setEnabled(True)
        self.manual_start_btn.setEnabled(True)
        self.btn_vacuum.setEnabled(True)

    def copy_to_clipboard(self):
        text = self.manual_output.toPlainText()
        if text:
            QGuiApplication.clipboard().setText(text)
            logging.info("Результат скопирован.")
        else:
            QMessageBox.warning(self, "Пусто", "Нечего копировать.")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = NoCloneGUI()
    window.show()
    sys.exit(app.exec())
