# Использование модуля db.py

## Описание

Модуль `db.py` обеспечивает работу с базой данных MySQL для миграции файлов. Включает в себя управление соединениями, выполнение запросов и специализированные методы для работы с таблицей `repl_AV_ATF`.

## Основные возможности

- ✅ Пул соединений с MySQL
- ✅ Автоматическое управление соединениями
- ✅ Специализированные методы для миграции
- ✅ Обработка ошибок и логирование
- ✅ Контекстный менеджер
- ✅ Статистика и аналитика

## Быстрый старт

```python
from src.config_loader import load_config
from src.logger import FileMigratorLogger
from src.db import Database

# Загрузка конфигурации и настройка логгера
config = load_config()
logger = FileMigratorLogger(config.logging)

# Создание объекта БД
db = Database(config.database, logger)

# Тестирование подключения
if db.test_connection():
    print("✅ Подключение к БД успешно!")
    
    # Получение статистики
    stats = db.get_migration_statistics()
    print(f"📊 Статистика: {stats}")
    
# Закрытие соединений
db.close()
```

## Основные методы

### Подключение и тестирование

```python
# Тестирование подключения
if db.test_connection():
    print("БД доступна")

# Получение статистики
stats = db.get_migration_statistics()
print(f"Всего файлов: {stats['total_files']}")
print(f"Перемещено: {stats['moved_files']}")
print(f"Не перемещено: {stats['unmoved_files']}")
```

### Работа с файлами

```python
# Получение файлов для миграции
files = db.get_files_to_move(batch_size=100)
for file_info in files:
    print(f"Файл: {file_info['IDFL']} - {file_info['filename']}")

# Получение метаданных конкретного файла
metadata = db.get_file_metadata("file001")
if metadata:
    print(f"Файл перемещен: {metadata['ismooved']}")
    print(f"Дата перемещения: {metadata['dtmoove']}")

# Отметка файла как перемещенного
success = db.mark_file_moved("file001")
if success:
    print("Файл отмечен как перемещенный")

# Добавление нового файла
success = db.insert_new_file("new_file", "document.pdf")
if success:
    print("Новый файл добавлен")
```

### Статистика и аналитика

```python
# Общее количество файлов
total = db.get_total_files_count()
print(f"Всего файлов: {total}")

# Количество не перемещенных файлов
unmoved = db.get_unmoved_files_count()
print(f"Не перемещено: {unmoved}")

# Файлы в диапазоне дат
from datetime import datetime
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)
files = db.get_files_by_date_range(start_date, end_date)
print(f"Файлов в январе: {len(files)}")

# Полная статистика миграции
stats = db.get_migration_statistics()
print(f"Статистика: {stats}")
```

## Контекстный менеджер

```python
# Автоматическое управление соединениями
with Database(config.database, logger) as db:
    if db.test_connection():
        files = db.get_files_to_move(10)
        for file_info in files:
            # Обработка файла
            success = db.mark_file_moved(file_info['IDFL'])
            if success:
                print(f"Файл {file_info['IDFL']} обработан")
# Соединения автоматически закрываются
```

## Обработка ошибок

```python
from src.db import DatabaseConnectionError, DatabaseQueryError

try:
    db = Database(config.database, logger)
    files = db.get_files_to_move(100)
    
except DatabaseConnectionError as e:
    print(f"Ошибка подключения: {e}")
    
except DatabaseQueryError as e:
    print(f"Ошибка запроса: {e}")
    
finally:
    if 'db' in locals():
        db.close()
```

## Структура таблицы repl_AV_ATF

```sql
CREATE TABLE repl_AV_ATF (
    IDFL VARCHAR(50) PRIMARY KEY,            -- Системное имя файла
    dt DATETIME NOT NULL,                    -- Дата помещения в базу
    filename VARCHAR(255) NOT NULL,          -- Пользовательское имя файла
    ismooved BOOLEAN DEFAULT FALSE,          -- Признак перемещения
    dtmoove DATETIME NULL,                   -- Дата и время перемещения
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

## Примеры использования

### Полный цикл миграции

```python
from src.config_loader import load_config
from src.logger import FileMigratorLogger
from src.db import Database

config = load_config()
logger = FileMigratorLogger(config.logging)

with Database(config.database, logger) as db:
    if not db.test_connection():
        logger.log_critical_error("Не удалось подключиться к БД")
        exit(1)
    
    # Получаем статистику
    stats = db.get_migration_statistics()
    logger.log_migration_start(
        total_files=stats['unmoved_files'],
        batch_size=config.migrator.batch_size
    )
    
    # Обрабатываем файлы батчами
    batch_number = 1
    while True:
        files = db.get_files_to_move(config.migrator.batch_size)
        if not files:
            break
        
        logger.log_batch_start(batch_number, len(files))
        
        successful = 0
        failed = 0
        
        for file_info in files:
            try:
                # Здесь должна быть логика перемещения файла
                # file_ops.move_file(file_info['IDFL'], file_info['dt'])
                
                # Отмечаем файл как перемещенный
                if db.mark_file_moved(file_info['IDFL']):
                    successful += 1
                    logger.log_file_moved(
                        file_info['IDFL'],
                        Path("old_path"),
                        Path("new_path")
                    )
                else:
                    failed += 1
                    
            except Exception as e:
                failed += 1
                logger.log_file_error(file_info['IDFL'], e)
        
        logger.log_batch_end(batch_number, len(files), successful, failed)
        batch_number += 1
    
    # Финальная статистика
    final_stats = db.get_migration_statistics()
    logger.log_migration_end(
        processed=stats['total_files'],
        successful=final_stats['moved_files'],
        failed=final_stats['unmoved_files']
    )
```

### Поиск и анализ файлов

```python
# Поиск файлов по дате
from datetime import datetime, timedelta

start_date = datetime.now() - timedelta(days=30)
end_date = datetime.now()

recent_files = db.get_files_by_date_range(start_date, end_date)
print(f"Файлов за последние 30 дней: {len(recent_files)}")

# Анализ по типам файлов
for file_info in recent_files:
    filename = file_info['filename']
    extension = filename.split('.')[-1] if '.' in filename else 'no_extension'
    print(f"Файл: {filename}, расширение: {extension}")

# Поиск проблемных файлов
unmoved_files = db.get_files_to_move(1000)  # Получаем все не перемещенные
old_files = [f for f in unmoved_files if f['dt'] < datetime.now() - timedelta(days=7)]
print(f"Старых не перемещенных файлов: {len(old_files)}")
```

### Мониторинг и диагностика

```python
# Проверка состояния БД
def check_database_health(db):
    """Проверяет состояние базы данных."""
    try:
        if not db.test_connection():
            return False, "Нет подключения к БД"
        
        stats = db.get_migration_statistics()
        
        # Проверяем аномалии
        if stats['total_files'] == 0:
            return False, "Нет файлов в БД"
        
        if stats['unmoved_files'] > stats['total_files']:
            return False, "Некорректные данные: не перемещенных больше чем всего"
        
        return True, "БД в порядке"
        
    except Exception as e:
        return False, f"Ошибка проверки БД: {e}"

# Использование
is_healthy, message = check_database_health(db)
if is_healthy:
    print(f"✅ {message}")
else:
    print(f"❌ {message}")
```

## Конфигурация

### DatabaseConfig

```python
from src.config_loader import DatabaseConfig

config = DatabaseConfig(
    driver='mysql',
    host='localhost',
    port=3306,
    database='FileMigratorTest',
    username='migrator_user',
    password='migrator_pass123'
)
```

### Настройка в settings.ini

```ini
[database]
driver = mysql
host = localhost
port = 3306
database = FileMigratorTest
username = migrator_user
password = migrator_pass123
```

## Производительность

### Пул соединений

Модуль использует пул соединений для оптимизации производительности:

- **Размер пула**: 5 соединений (настраивается)
- **Автоматическое управление**: соединения создаются и закрываются автоматически
- **Безопасность**: каждое соединение изолировано

### Оптимизация запросов

- Используются индексы на полях `dt`, `ismooved`, `dtmoove`
- Запросы оптимизированы для работы с большими объемами данных
- Поддержка батчевой обработки

## Тестирование

Запуск тестов:
```bash
python -m pytest tests/test_db.py -v
```

Тесты покрывают:
- Подключение к БД
- Выполнение запросов
- Все специализированные методы
- Обработку ошибок
- Контекстный менеджер
- Пул соединений
