# Использование модуля migrator.py

## Описание

Модуль `migrator.py` является центральным компонентом системы миграции файлов. Он объединяет работу с базой данных и файловой системой для выполнения миграции файлов из плоской структуры в структуру по датам (YYYYMMDD).

## Основные возможности

- ✅ Миграция файлов батчами с настраиваемым размером
- ✅ Проверка целостности файлов через хеширование
- ✅ Миграция по диапазону дат
- ✅ Детальная статистика и мониторинг процесса
- ✅ Проверка корректности миграции
- ✅ Обработка ошибок с детальным логированием
- ✅ Контекстный менеджер для безопасной работы

## Быстрый старт

```python
from src.config_loader import load_config
from src.logger import FileMigratorLogger
from src.migrator import Migrator

# Загрузка конфигурации и настройка логгера
config = load_config()
logger = FileMigratorLogger(config.logging)

# Создание мигратора
migrator = Migrator(config, logger)

# Инициализация и проверка готовности
if migrator.initialize():
    print("✅ Мигратор готов к работе")
    
    # Получение статуса
    status = migrator.get_migration_status()
    print(f"📊 Статус: {status['database']['unmoved_files']} файлов для миграции")
else:
    print("❌ Ошибка инициализации мигратора")
```

## Основные методы

### Инициализация и статус

```python
# Инициализация мигратора
if migrator.initialize():
    print("Мигратор готов к работе")

# Получение текущего статуса
status = migrator.get_migration_status()
print(f"Всего файлов: {status['database']['total_files']}")
print(f"Перемещено: {status['database']['moved_files']}")
print(f"Не перемещено: {status['database']['unmoved_files']}")
```

### Миграция батчами

```python
# Миграция одного батча
processed, successful, failed = migrator.migrate_batch(batch_size=100)
print(f"Обработано: {processed}, Успешно: {successful}, Ошибок: {failed}")

# Миграция всех файлов
stats = migrator.migrate_all()
print(f"Общая статистика: {stats.to_dict()}")
```

### Миграция по диапазону дат

```python
from datetime import datetime

# Миграция файлов за определенный период
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 1, 31)
stats = migrator.migrate_by_date_range(start_date, end_date)

print(f"Миграция за январь 2024:")
print(f"  • Обработано: {stats.processed_files}")
print(f"  • Успешно: {stats.successful_files}")
print(f"  • Ошибок: {stats.failed_files}")
print(f"  • Продолжительность: {stats.get_duration():.2f} сек")
```

### Проверка миграции

```python
# Проверка корректности миграции на выборке
verification_result = migrator.verify_migration(sample_size=50)
print(f"Проверено файлов: {verification_result['total_checked']}")
print(f"Корректных: {verification_result['verified']}")
print(f"Ошибок: {verification_result['errors']}")

if verification_result['errors'] > 0:
    print("Детали ошибок:")
    for detail in verification_result['details']:
        print(f"  • {detail}")
```

## Примеры использования

### Полный цикл миграции

```python
from src.config_loader import load_config
from src.logger import FileMigratorLogger
from src.migrator import Migrator

def full_migration():
    """Выполняет полную миграцию всех файлов."""
    try:
        # Инициализация
        config = load_config()
        logger = FileMigratorLogger(config.logging)
        
        with Migrator(config, logger) as migrator:
            # Проверяем готовность
            if not migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return False
            
            # Получаем начальную статистику
            initial_status = migrator.get_migration_status()
            total_files = initial_status['database']['unmoved_files']
            
            print(f"🚀 Начало миграции {total_files} файлов")
            
            # Выполняем миграцию
            stats = migrator.migrate_all()
            
            # Выводим результаты
            print(f"✅ Миграция завершена!")
            print(f"📊 Статистика:")
            print(f"   • Обработано: {stats.processed_files}")
            print(f"   • Успешно: {stats.successful_files}")
            print(f"   • Ошибок: {stats.failed_files}")
            print(f"   • Процент успеха: {stats.get_success_rate():.1f}%")
            print(f"   • Продолжительность: {stats.get_duration():.2f} сек")
            
            # Проверяем результат
            if stats.failed_files > 0:
                print(f"⚠️ Обнаружено {stats.failed_files} ошибок:")
                for error in stats.errors[:5]:  # Показываем первые 5 ошибок
                    print(f"   • {error['file_id']}: {error['error']}")
            
            return stats.failed_files == 0
            
    except Exception as e:
        print(f"❌ Критическая ошибка миграции: {e}")
        return False

# Использование
success = full_migration()
print(f"Миграция {'успешна' if success else 'завершена с ошибками'}")
```

### Миграция с мониторингом прогресса

```python
def migration_with_progress():
    """Миграция с отображением прогресса."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    with Migrator(config, logger) as migrator:
        if not migrator.initialize():
            return False
        
        # Получаем общее количество файлов
        status = migrator.get_migration_status()
        total_files = status['database']['unmoved_files']
        
        print(f"📁 Всего файлов для миграции: {total_files}")
        
        # Мигрируем батчами с отображением прогресса
        batch_size = 100
        processed_total = 0
        
        while processed_total < total_files:
            processed, successful, failed = migrator.migrate_batch(batch_size)
            
            if processed == 0:
                break  # Нет больше файлов для миграции
            
            processed_total += processed
            progress = (processed_total / total_files) * 100
            
            print(f"📈 Прогресс: {processed_total}/{total_files} ({progress:.1f}%)")
            print(f"   • Батч: {processed} файлов")
            print(f"   • Успешно: {successful}")
            print(f"   • Ошибок: {failed}")
            
            # Небольшая пауза между батчами
            time.sleep(1)
        
        print("✅ Миграция завершена!")
        return True

# Использование
migration_with_progress()
```

### Миграция по расписанию

```python
from datetime import datetime, timedelta

def scheduled_migration():
    """Миграция файлов за последние 7 дней."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    with Migrator(config, logger) as migrator:
        if not migrator.initialize():
            return False
        
        # Определяем диапазон дат (последние 7 дней)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print(f"📅 Миграция файлов за период: {start_date.date()} - {end_date.date()}")
        
        # Выполняем миграцию
        stats = migrator.migrate_by_date_range(start_date, end_date)
        
        print(f"📊 Результаты миграции за неделю:")
        print(f"   • Обработано: {stats.processed_files}")
        print(f"   • Успешно: {stats.successful_files}")
        print(f"   • Ошибок: {stats.failed_files}")
        
        return stats.failed_files == 0

# Использование
scheduled_migration()
```

### Проверка и восстановление

```python
def verify_and_repair():
    """Проверяет миграцию и выводит отчет."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    with Migrator(config, logger) as migrator:
        if not migrator.initialize():
            return False
        
        print("🔍 Проверка корректности миграции...")
        
        # Проверяем на выборке файлов
        verification_result = migrator.verify_migration(sample_size=200)
        
        print(f"📋 Отчет о проверке:")
        print(f"   • Проверено файлов: {verification_result['total_checked']}")
        print(f"   • Корректных: {verification_result['verified']}")
        print(f"   • Ошибок: {verification_result['errors']}")
        
        if verification_result['errors'] > 0:
            print(f"\n⚠️ Обнаружены проблемы:")
            for detail in verification_result['details']:
                print(f"   • {detail}")
            
            # Здесь можно добавить логику восстановления
            print("\n🔧 Рекомендуется выполнить повторную миграцию проблемных файлов")
        else:
            print("✅ Все проверенные файлы корректны!")
        
        return verification_result['errors'] == 0

# Использование
verify_and_repair()
```

### Работа с контекстным менеджером

```python
def safe_migration():
    """Безопасная миграция с автоматической очисткой ресурсов."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    try:
        # Используем контекстный менеджер для автоматической очистки
        with Migrator(config, logger) as migrator:
            if not migrator.initialize():
                return False
            
            # Выполняем миграцию
            stats = migrator.migrate_all(max_files=1000)  # Ограничиваем количество
            
            print(f"✅ Миграция завершена: {stats.successful_files} файлов")
            
            # Ресурсы автоматически очищаются при выходе из контекста
            return True
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

# Использование
safe_migration()
```

## Статистика и мониторинг

### Получение детальной статистики

```python
def get_detailed_stats():
    """Получает детальную статистику миграции."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    with Migrator(config, logger) as migrator:
        if not migrator.initialize():
            return
        
        # Получаем статус
        status = migrator.get_migration_status()
        
        print("📊 Детальная статистика миграции:")
        print(f"\n🗄️ База данных:")
        db_stats = status['database']
        print(f"   • Всего файлов: {db_stats['total_files']}")
        print(f"   • Перемещено: {db_stats['moved_files']}")
        print(f"   • Не перемещено: {db_stats['unmoved_files']}")
        print(f"   • Первый файл: {db_stats.get('earliest_file_date', 'N/A')}")
        print(f"   • Последний файл: {db_stats.get('latest_file_date', 'N/A')}")
        
        print(f"\n📁 Файловая система:")
        fs_stats = status['filesystem']
        print(f"   • Не перемещенных файлов: {fs_stats['unmoved_files_count']}")
        print(f"   • Размер не перемещенных: {fs_stats['unmoved_files_size']} байт")
        print(f"   • Перемещенных файлов: {fs_stats['moved_files_count']}")
        print(f"   • Размер перемещенных: {fs_stats['moved_files_size']} байт")
        print(f"   • Каталогов по датам: {fs_stats['date_directories_count']}")
        
        print(f"\n⏰ Время: {status['timestamp']}")

# Использование
get_detailed_stats()
```

### Мониторинг в реальном времени

```python
import time

def real_time_monitoring():
    """Мониторинг миграции в реальном времени."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    with Migrator(config, logger) as migrator:
        if not migrator.initialize():
            return
        
        print("🔄 Запуск мониторинга миграции...")
        
        # Получаем начальное состояние
        initial_status = migrator.get_migration_status()
        initial_unmoved = initial_status['database']['unmoved_files']
        
        print(f"📊 Начальное состояние: {initial_unmoved} файлов для миграции")
        
        # Мигрируем с мониторингом
        batch_size = 50
        processed_total = 0
        
        while True:
            processed, successful, failed = migrator.migrate_batch(batch_size)
            
            if processed == 0:
                print("✅ Все файлы обработаны!")
                break
            
            processed_total += processed
            remaining = initial_unmoved - processed_total
            
            print(f"📈 Обработано: {processed_total}/{initial_unmoved} "
                  f"({(processed_total/initial_unmoved)*100:.1f}%)")
            print(f"   • Осталось: {remaining} файлов")
            print(f"   • Последний батч: {successful} успешно, {failed} ошибок")
            
            # Пауза между батчами
            time.sleep(2)

# Использование
real_time_monitoring()
```

## Обработка ошибок

```python
from src.migrator import MigrationError

def error_handling_example():
    """Пример обработки ошибок при миграции."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    try:
        with Migrator(config, logger) as migrator:
            if not migrator.initialize():
                raise MigrationError("Не удалось инициализировать мигратор")
            
            # Выполняем миграцию
            stats = migrator.migrate_all()
            
            # Проверяем результат
            if stats.failed_files > 0:
                print(f"⚠️ Обнаружено {stats.failed_files} ошибок:")
                for error in stats.errors:
                    print(f"   • {error['file_id']}: {error['error']}")
                
                # Можно добавить логику повторной попытки
                print("🔄 Попытка повторной миграции проблемных файлов...")
            
            return stats.failed_files == 0
            
    except MigrationError as e:
        print(f"❌ Ошибка миграции: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

# Использование
success = error_handling_example()
print(f"Результат: {'успех' if success else 'ошибка'}")
```

## Конфигурация

### Настройка параметров миграции

```ini
[migrator]
batch_size = 1000          # Размер батча для обработки
max_retries = 3            # Максимальное количество попыток
retry_delay = 1            # Задержка между попытками (секунды)
```

### Программная настройка

```python
from src.config_loader import Config, MigratorConfig

# Создание конфигурации программно
migrator_config = MigratorConfig(
    batch_size=500,        # Меньший размер батча
    max_retries=5,         # Больше попыток
    retry_delay=2          # Больше задержка
)

# Обновление конфигурации
config.migrator = migrator_config
```

## Производительность

### Оптимизация параметров

- **batch_size**: Увеличьте для лучшей производительности, уменьшите для стабильности
- **retry_delay**: Настройте в зависимости от нагрузки на систему
- **max_retries**: Увеличьте для нестабильных сетей/дисков

### Мониторинг производительности

```python
def performance_monitoring():
    """Мониторинг производительности миграции."""
    config = load_config()
    logger = FileMigratorLogger(config.logging)
    
    with Migrator(config, logger) as migrator:
        if not migrator.initialize():
            return
        
        start_time = time.time()
        
        # Выполняем миграцию
        stats = migrator.migrate_all()
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Вычисляем метрики производительности
        files_per_second = stats.processed_files / total_duration if total_duration > 0 else 0
        success_rate = stats.get_success_rate()
        
        print(f"📊 Метрики производительности:")
        print(f"   • Файлов в секунду: {files_per_second:.2f}")
        print(f"   • Процент успеха: {success_rate:.1f}%")
        print(f"   • Общее время: {total_duration:.2f} сек")
        print(f"   • Среднее время на файл: {total_duration/stats.processed_files:.3f} сек")

# Использование
performance_monitoring()
```

## Тестирование

Запуск тестов:
```bash
python -m pytest tests/test_migrator.py -v
```

Тесты покрывают:
- Инициализацию и конфигурацию
- Миграцию батчами и отдельных файлов
- Обработку ошибок и исключений
- Статистику и мониторинг
- Проверку целостности
- Контекстный менеджер
