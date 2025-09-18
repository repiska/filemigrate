# Использование модуля file_ops.py

## Описание

Модуль `file_ops.py` обеспечивает операции с файловой системой для миграции файлов. Включает в себя перемещение файлов в структуру по датам (YYYYMMDD), чтение и запись файлов с поддержкой старой и новой схемы организации.

## Основные возможности

- ✅ Перемещение файлов в структуру по датам
- ✅ Чтение файлов по старой и новой схеме
- ✅ Запись новых файлов в новую структуру
- ✅ Проверка существования и получение метаданных файлов
- ✅ Хеширование файлов для проверки целостности
- ✅ Статистика использования дискового пространства
- ✅ Очистка пустых каталогов

## Быстрый старт

```python
from src.config_loader import load_config
from src.logger import FileMigratorLogger
from src.file_ops import FileOps

# Загрузка конфигурации и настройка логгера
config = load_config()
logger = FileMigratorLogger(config.logging)

# Создание объекта операций с файлами
file_ops = FileOps(config.paths, logger)

# Получение статистики
stats = file_ops.get_storage_statistics()
print(f"📊 Статистика: {stats}")
```

## Основные методы

### Перемещение файлов

```python
from datetime import datetime

# Перемещение файла в структуру по дате
test_date = datetime(2024, 1, 15, 10, 30)
new_path = file_ops.move_file("file001", test_date)
print(f"Файл перемещен в: {new_path}")

# Файл будет перемещен в: new_base_path/20240115/file001
```

### Чтение файлов

```python
# Чтение по старой схеме (файл не перемещен)
content = file_ops.read_file("file001", ismooved=False, dt=datetime.now())
print(f"Размер файла: {len(content)} байт")

# Чтение по новой схеме (файл перемещен)
test_date = datetime(2024, 1, 15, 10, 30)
content = file_ops.read_file("file001", ismooved=True, dt=test_date)
print(f"Содержимое: {content.decode('utf-8')}")
```

### Запись файлов

```python
# Запись нового файла в новую структуру
test_date = datetime(2024, 1, 15, 10, 30)
file_content = b"Новое содержимое файла"
new_path = file_ops.write_file("new_file.txt", "new_file.txt", file_content, test_date)
print(f"Новый файл создан: {new_path}")
```

### Проверка существования и метаданные

```python
# Проверка существования файла
exists = file_ops.file_exists("file001", ismooved=False, dt=datetime.now())
print(f"Файл существует: {exists}")

# Получение размера файла
size = file_ops.get_file_size("file001", ismooved=False, dt=datetime.now())
print(f"Размер файла: {size} байт")

# Получение хеша файла
file_hash = file_ops.get_file_hash("file001", ismooved=False, dt=datetime.now(), algorithm="md5")
print(f"MD5 хеш: {file_hash}")
```

## Структура каталогов

### Старая схема (до миграции)
```
base_path/
├── file001
├── file002
├── file003
└── ...
```

### Новая схема (после миграции)
```
new_base_path/
├── 20240115/
│   ├── file001
│   └── file002
├── 20240116/
│   ├── file003
│   └── file004
└── 20240117/
    └── file005
```

## Примеры использования

### Полный цикл миграции файла

```python
from datetime import datetime
from src.config_loader import load_config
from src.logger import FileMigratorLogger
from src.file_ops import FileOps

config = load_config()
logger = FileMigratorLogger(config.logging)
file_ops = FileOps(config.paths, logger)

def migrate_file(idfl: str, dt: datetime):
    """Мигрирует файл из старой структуры в новую."""
    try:
        # Проверяем, что файл существует в старой структуре
        if not file_ops.file_exists(idfl, ismooved=False, dt=dt):
            logger.log_file_error(idfl, FileNotFoundError("Файл не найден в старой структуре"))
            return False
        
        # Читаем файл из старой структуры
        content = file_ops.read_file(idfl, ismooved=False, dt=dt)
        logger.log_system_info(f"Файл {idfl} прочитан, размер: {len(content)} байт")
        
        # Перемещаем файл в новую структуру
        new_path = file_ops.move_file(idfl, dt)
        logger.log_file_moved(idfl, Path("old_path"), new_path)
        
        # Проверяем целостность
        old_hash = file_ops.get_file_hash(idfl, ismooved=False, dt=dt, algorithm="md5")
        new_hash = file_ops.get_file_hash(idfl, ismooved=True, dt=dt, algorithm="md5")
        
        if old_hash == new_hash:
            logger.log_system_info(f"Целостность файла {idfl} проверена")
            return True
        else:
            logger.log_file_error(idfl, ValueError("Ошибка целостности файла"))
            return False
            
    except Exception as e:
        logger.log_file_error(idfl, e)
        return False

# Использование
test_date = datetime(2024, 1, 15, 10, 30)
success = migrate_file("file001", test_date)
print(f"Миграция {'успешна' if success else 'неудачна'}")
```

### Анализ файлового хранилища

```python
def analyze_storage(file_ops):
    """Анализирует использование файлового хранилища."""
    stats = file_ops.get_storage_statistics()
    
    print("📊 Анализ файлового хранилища:")
    print(f"   • Не перемещенных файлов: {stats['unmoved_files_count']}")
    print(f"   • Размер не перемещенных: {stats['unmoved_files_size']} байт")
    print(f"   • Перемещенных файлов: {stats['moved_files_count']}")
    print(f"   • Размер перемещенных: {stats['moved_files_size']} байт")
    print(f"   • Каталогов по датам: {stats['date_directories_count']}")
    
    # Анализ по датам
    unmoved_files = file_ops.list_unmoved_files()
    print(f"\n📁 Не перемещенные файлы ({len(unmoved_files)}):")
    for file_path in unmoved_files[:10]:  # Показываем первые 10
        size = file_path.stat().st_size
        print(f"   • {file_path.name} ({size} байт)")
    
    if len(unmoved_files) > 10:
        print(f"   ... и еще {len(unmoved_files) - 10} файлов")

# Использование
analyze_storage(file_ops)
```

### Очистка и обслуживание

```python
def cleanup_storage(file_ops):
    """Выполняет очистку файлового хранилища."""
    logger = file_ops.logger
    
    # Очистка пустых каталогов
    removed_dirs = file_ops.cleanup_empty_directories()
    logger.log_system_info(f"Удалено пустых каталогов: {removed_dirs}")
    
    # Получение статистики после очистки
    stats = file_ops.get_storage_statistics()
    logger.log_system_info(f"Статистика после очистки: {stats}")
    
    return removed_dirs

# Использование
removed_count = cleanup_storage(file_ops)
print(f"Удалено пустых каталогов: {removed_count}")
```

### Проверка целостности файлов

```python
def verify_file_integrity(file_ops, idfl: str, dt: datetime, ismooved: bool):
    """Проверяет целостность файла."""
    try:
        # Получаем хеш файла
        file_hash = file_ops.get_file_hash(idfl, ismooved, dt, algorithm="sha256")
        
        if file_hash is None:
            return False, "Файл не найден"
        
        # Получаем размер файла
        file_size = file_ops.get_file_size(idfl, ismooved, dt)
        
        if file_size is None:
            return False, "Не удалось получить размер файла"
        
        # Читаем файл и проверяем размер
        content = file_ops.read_file(idfl, ismooved, dt)
        if len(content) != file_size:
            return False, "Размер файла не соответствует"
        
        return True, f"Файл целостен (SHA256: {file_hash[:16]}...)"
        
    except Exception as e:
        return False, f"Ошибка проверки: {e}"

# Использование
test_date = datetime(2024, 1, 15, 10, 30)
is_valid, message = verify_file_integrity(file_ops, "file001", test_date, False)
print(f"Проверка файла: {'✅' if is_valid else '❌'} {message}")
```

### Массовая обработка файлов

```python
def batch_process_files(file_ops, file_list, dt: datetime):
    """Обрабатывает список файлов батчами."""
    logger = file_ops.logger
    successful = 0
    failed = 0
    
    logger.log_system_info(f"Начало обработки {len(file_list)} файлов")
    
    for i, file_info in enumerate(file_list):
        try:
            idfl = file_info['IDFL']
            
            # Проверяем существование файла
            if not file_ops.file_exists(idfl, ismooved=False, dt=dt):
                logger.log_file_error(idfl, FileNotFoundError("Файл не найден"))
                failed += 1
                continue
            
            # Перемещаем файл
            new_path = file_ops.move_file(idfl, dt)
            logger.log_file_moved(idfl, Path("old_path"), new_path)
            successful += 1
            
            # Логируем прогресс каждые 100 файлов
            if (i + 1) % 100 == 0:
                logger.log_progress(i + 1, len(file_list))
                
        except Exception as e:
            logger.log_file_error(file_info.get('IDFL', 'unknown'), e)
            failed += 1
    
    logger.log_system_info(f"Обработка завершена: {successful} успешно, {failed} ошибок")
    return successful, failed

# Использование
from src.db import Database

# Получаем файлы из БД
with Database(config.database, logger) as db:
    files = db.get_files_to_move(1000)  # Получаем 1000 файлов

# Обрабатываем файлы
successful, failed = batch_process_files(file_ops, files, datetime.now())
print(f"Результат: {successful} успешно, {failed} ошибок")
```

## Обработка ошибок

```python
from src.file_ops import FileOperationError, FileNotFoundError

try:
    # Операция с файлом
    content = file_ops.read_file("file001", False, datetime.now())
    
except FileNotFoundError as e:
    print(f"Файл не найден: {e}")
    
except FileOperationError as e:
    print(f"Ошибка операции с файлом: {e}")
    
except Exception as e:
    print(f"Неожиданная ошибка: {e}")
```

## Конфигурация

### PathsConfig

```python
from src.config_loader import PathsConfig
from pathlib import Path

config = PathsConfig(
    file_path=Path("test_files"),      # Базовый путь к файлам
    new_file_path=Path("test_files")   # Путь для новой структуры
)
```

### Настройка в settings.ini

```ini
[paths]
file_path = test_files
new_file_path = test_files
```

## Производительность

### Оптимизация операций

- **Батчевая обработка**: обрабатывайте файлы группами для лучшей производительности
- **Проверка существования**: используйте `file_exists()` перед операциями чтения/записи
- **Хеширование**: используйте для проверки целостности при критических операциях
- **Очистка**: регулярно удаляйте пустые каталоги

### Мониторинг

```python
# Получение статистики для мониторинга
stats = file_ops.get_storage_statistics()

# Проверка использования дискового пространства
total_size = stats['unmoved_files_size'] + stats['moved_files_size']
print(f"Общее использование: {total_size} байт")

# Проверка прогресса миграции
migration_progress = stats['moved_files_count'] / (stats['moved_files_count'] + stats['unmoved_files_count'])
print(f"Прогресс миграции: {migration_progress:.1%}")
```

## Тестирование

Запуск тестов:
```bash
python -m pytest tests/test_file_ops.py -v
```

Тесты покрывают:
- Инициализацию и создание каталогов
- Перемещение файлов
- Чтение и запись файлов
- Проверку существования и метаданные
- Хеширование файлов
- Статистику и очистку
- Обработку ошибок
