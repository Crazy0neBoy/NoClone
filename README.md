# NoClone

Утилита для очистки текстовых файлов от повторяющихся строк. Подходит для списков идентификаторов и любых наборов, где важно оставить только уникальные значения.

## Возможности
- Сохраняет только новые строки, игнорируя дубликаты
- Ведёт статистику обработанных и повторяющихся строк
- Формирует чистый список без повторов (`GoodId.txt`) и пополняет базу (`IdsBD.txt`)

## Установка
```bash
git clone https://github.com/Crazy0neBoy/NoClone
cd NoClone
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\Activate.bat
pip install -r requirements.txt
```

## Использование
1. Заполните файл `input.txt` строками для проверки.
2. Запустите скрипт:
   ```bash
   python main.py
   ```
   или на Windows можно использовать `Start_NoClone.bat`.
3. Результаты:
   - `IdsBD.txt` – накопительная база уникальных строк;
   - `GoodId.txt` – новые уникальные строки текущего запуска;
   - `input.txt` очищается после обработки.

## Лицензия
Проект распространяется по лицензии MIT (см. файл `LICENSE`).

---

### English
NoClone removes duplicate lines from text files. Put lines into `input.txt` and run `python main.py` to collect unique values. New unique lines are saved to `GoodId.txt` and accumulated in `IdsBD.txt`. Requires Python 3.10+ and the `tqdm` package.
