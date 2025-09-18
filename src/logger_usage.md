# Использование модуля logger.py

## Описание

Модуль `logger.py` обеспечивает централизованную настройку и управление логированием приложения File Migrator с поддержкой цветного вывода, ротации файлов и специализированных методов для различных операций.

## Основные возможности

- ✅ Цветной вывод в консоль с эмодзи
- ✅ Ротация файлов логов
- ✅ Специализированные методы для миграции
- ✅ Настройка уровней логирования
- ✅ Обработка ошибок и предупреждений
- ✅ Прогресс-бар и статистика

## Быстрый старт

```python
from src.config_loader import load_config
from src.logger import setup_logger

# Загрузка конфигурации и настройка логгера
config = load_config()
logger = setup_logger(config.logging)

# Базовое логирование
logger.info("Приложение запущено")
logger.warning("Предупреждение")
logger.error("Ошибка")
```

## Специализированные методы

### Миграция файлов

```python
from src.logger import FileMigratorLogger

migrator_logger = FileMigratorLogger(config.logging)

# Начало миграции
migrator_logger.log_migration_start(total_files=1000, batch_size=100)

# Перемещение файла
migrator_logger.log_file_moved("file001", Path("old/path"), Path("new/path"))

# Прогресс
migrator_logger.log_progress(current=50, total=100)

# Завершение миграции
migrator_logger.log_migration_end(processed=100, successful=95, failed=5)
```

### Обработка батчей

```python
# Начало батча
migrator_logger.log_batch_start(batch_number=1, batch_size=50)

# Завершение батча
migrator_logger.log_batch_end(batch_number=1, processed=50, successful=48, failed=2)
```

### Ошибки и операции

```python
# Ошибка файла
migrator_logger.log_file_error("file001", FileNotFoundError("File not found"))

# Ошибка БД
migrator_logger.log_database_error("SELECT", ConnectionError("Connection failed"))

# Операции с файлами
migrator_logger.log_file_operation("read", Path("file.txt"), success=True)
migrator_logger.log_file_operation("write", Path("file.txt"), success=False)

# Системная информация
migrator_logger.log_system_info("Система запущена")
migrator_logger.log_warning("Низкое место на диске")
migrator_logger.log_critical_error("Критическая ошибка", RuntimeError("System failure"))
```

## Конфигурация

### LoggingConfig

```python
from src.config_loader import LoggingConfig
from pathlib import Path

config = LoggingConfig(
    level='INFO',                    # DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_file=Path('logs/app.log'),   # Путь к файлу логов
    max_log_size=10,                 # Максимальный размер файла (MB)
    backup_count=5                   # Количество файлов для ротации
)
```

### Настройка в settings.ini

```ini
[logging]
level = INFO
log_file = logs/migrator.log
max_log_size = 10
backup_count = 5
```

## Форматы вывода

### Консольный вывод (с цветами и эмодзи)
```
2025-09-18 01:43:58 [INFO] file_migrator: ℹ️ Информационное сообщение
2025-09-18 01:43:58 [WARNING] file_migrator: ⚠️ Предупреждение
2025-09-18 01:43:58 [ERROR] file_migrator: ❌ Ошибка
2025-09-18 01:43:58 [CRITICAL] file_migrator: 💥 Критическая ошибка
```

### Файловый вывод (без цветов)
```
2025-09-18 01:43:58 [INFO] file_migrator: ℹ️ Информационное сообщение
2025-09-18 01:43:58 [WARNING] file_migrator: ⚠️ Предупреждение
2025-09-18 01:43:58 [ERROR] file_migrator: ❌ Ошибка
2025-09-18 01:43:58 [CRITICAL] file_migrator: 💥 Критическая ошибка
```

## Специализированные сообщения

### Миграция
- 🚀 Начало миграции файлов
- 📊 Статистика (всего файлов, размер батча)
- ⏰ Время начала/завершения
- 📁 Перемещение файлов
- 📈 Прогресс выполнения
- ✅ Завершение миграции

### Операции
- 📦 Обработка батчей
- 🗄️ Операции с БД
- 📁 Операции с файлами
- ⚙️ Конфигурация
- ℹ️ Системная информация
- ⚠️ Предупреждения
- ❌ Ошибки
- 💥 Критические ошибки

## Ротация файлов

Логгер автоматически создает новые файлы при достижении максимального размера:

- `migrator.log` - текущий файл
- `migrator.log.1` - предыдущий файл
- `migrator.log.2` - файл на 2 позиции назад
- и т.д.

## Уровни логирования

- **DEBUG**: Отладочная информация
- **INFO**: Общая информация о работе
- **WARNING**: Предупреждения
- **ERROR**: Ошибки
- **CRITICAL**: Критические ошибки

## Примеры использования

### Полный цикл миграции

```python
from src.config_loader import load_config
from src.logger import FileMigratorLogger

config = load_config()
logger = FileMigratorLogger(config.logging)

try:
    # Начало миграции
    logger.log_migration_start(1000, 100)
    
    # Обработка файлов
    for i in range(10):
        logger.log_batch_start(i+1, 100)
        
        # Симуляция обработки
        for j in range(100):
            if j % 10 == 0:
                logger.log_progress(i*100 + j, 1000)
            
            if j % 50 == 0:
                logger.log_file_moved(f"file{j}", Path("old"), Path("new"))
        
        logger.log_batch_end(i+1, 100, 98, 2)
    
    # Завершение
    logger.log_migration_end(1000, 980, 20)
    
except Exception as e:
    logger.log_critical_error("Ошибка миграции", e)
```

### Обработка ошибок

```python
try:
    # Операция с файлом
    with open("file.txt", "r") as f:
        content = f.read()
    logger.log_file_operation("read", Path("file.txt"), True)
    
except FileNotFoundError as e:
    logger.log_file_error("file.txt", e)
except Exception as e:
    logger.log_critical_error("Неожиданная ошибка", e)
```

## Тестирование

Запуск тестов:
```bash
python -m pytest tests/test_logger.py -v
```

Тесты покрывают:
- Цветной форматтер
- Инициализацию логгера
- Все специализированные методы
- Обработку ошибок
- Ротацию файлов
- Различные уровни логирования
