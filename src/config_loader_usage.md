# Использование модуля config_loader.py

## Описание

Модуль `config_loader.py` обеспечивает централизованную загрузку и валидацию конфигурации приложения из файла `config/settings.ini`.

## Основные возможности

- ✅ Загрузка конфигурации из INI файла
- ✅ Валидация всех параметров
- ✅ Поддержка MySQL и SQL Server
- ✅ Типизированные объекты конфигурации
- ✅ Обработка ошибок
- ✅ Перезагрузка конфигурации

## Быстрый старт

```python
from src.config_loader import load_config

# Загрузка конфигурации
config = load_config()

# Использование настроек
print(f"База данных: {config.database.database}")
print(f"Путь к файлам: {config.paths.file_path}")
print(f"Размер батча: {config.migrator.batch_size}")
```

## Структура конфигурации

### DatabaseConfig
- `driver`: Драйвер БД (mysql/pyodbc)
- `host`: Хост БД
- `port`: Порт БД
- `database`: Имя БД
- `username`: Пользователь
- `password`: Пароль
- `trusted_connection`: Доверенное подключение (только для SQL Server)

### PathsConfig
- `file_path`: Базовый путь к файлам
- `new_file_path`: Путь для новых файлов

### MigratorConfig
- `batch_size`: Размер батча для обработки
- `max_retries`: Максимальное количество попыток
- `retry_delay`: Задержка между попытками (секунды)

### LoggingConfig
- `level`: Уровень логирования (DEBUG/INFO/WARNING/ERROR/CRITICAL)
- `log_file`: Путь к файлу логов
- `max_log_size`: Максимальный размер файла лога (MB)
- `backup_count`: Количество файлов логов для ротации

## Примеры использования

### Базовое использование
```python
from src.config_loader import load_config

config = load_config()
```

### Расширенное использование
```python
from src.config_loader import ConfigLoader

loader = ConfigLoader("custom_config.ini")
config = loader.load_config()

# Перезагрузка конфигурации
new_config = loader.reload_config()
```

### Обработка ошибок
```python
from src.config_loader import load_config
from FileNotFoundError import FileNotFoundError
from ValueError import ValueError

try:
    config = load_config()
except FileNotFoundError:
    print("Файл конфигурации не найден")
except ValueError as e:
    print(f"Ошибка конфигурации: {e}")
```

## Валидация

Модуль автоматически валидирует:
- Существование файла конфигурации
- Наличие всех обязательных секций
- Корректность значений параметров
- Существование путей к файлам
- Валидность уровней логирования

## Тестирование

Запуск тестов:
```bash
python -m pytest tests/test_config_loader.py -v
```

Тесты покрывают:
- Успешную загрузку конфигурации
- Обработку ошибок
- Валидацию параметров
- Поддержку разных драйверов БД
- Перезагрузку конфигурации
