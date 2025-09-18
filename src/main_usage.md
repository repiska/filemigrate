# Использование CLI интерфейса main.py

## Описание

Модуль `main.py` предоставляет удобный командный интерфейс для утилиты миграции файлов. Он объединяет все функциональные модули и предоставляет простые команды для выполнения различных операций.

## Основные возможности

- ✅ Миграция файлов (полная, батчами, по диапазону дат)
- ✅ Просмотр статуса миграции
- ✅ Проверка корректности миграции
- ✅ Очистка ресурсов
- ✅ Тестирование подключения к БД
- ✅ Просмотр списка файлов
- ✅ Подробная справка и примеры использования

## Быстрый старт

```bash
# Просмотр справки
python src/main.py --help

# Тестирование подключения к БД
python src/main.py test-connection

# Просмотр статуса миграции
python src/main.py status

# Миграция всех файлов
python src/main.py migrate
```

## Доступные команды

### 1. Миграция файлов

#### Полная миграция
```bash
# Миграция всех файлов
python src/main.py migrate

# Миграция с ограничением количества
python src/main.py migrate --max-files 1000
```

#### Миграция батчами
```bash
# Миграция одного батча (размер из конфигурации)
python src/main.py migrate-batch

# Миграция батча с указанным размером
python src/main.py migrate-batch --batch-size 50
```

#### Миграция по диапазону дат
```bash
# Миграция файлов за определенный период
python src/main.py migrate-date-range --start-date 2024-01-01 --end-date 2024-01-31

# Миграция файлов за один день
python src/main.py migrate-date-range --start-date 2024-01-15 --end-date 2024-01-15
```

### 2. Мониторинг и статус

#### Просмотр статуса
```bash
# Полный статус миграции
python src/main.py status
```

Выводит:
- Статистику базы данных (всего файлов, перемещено, не перемещено)
- Статистику файловой системы (размеры, количество каталогов)
- Прогресс миграции в процентах
- Время последнего обновления

#### Проверка корректности
```bash
# Проверка на выборке 100 файлов (по умолчанию)
python src/main.py verify

# Проверка на выборке 200 файлов
python src/main.py verify --sample-size 200
```

### 3. Управление файлами

#### Просмотр списка файлов
```bash
# Список не перемещенных файлов (первые 20)
python src/main.py list-files --type unmoved

# Список не перемещенных файлов (первые 10)
python src/main.py list-files --type unmoved --limit 10

# Список перемещенных файлов
python src/main.py list-files --type moved --limit 15
```

#### Очистка ресурсов
```bash
# Очистка пустых каталогов и освобождение ресурсов
python src/main.py cleanup
```

### 4. Диагностика

#### Тестирование подключения
```bash
# Проверка подключения к базе данных
python src/main.py test-connection
```

## Общие параметры

### Конфигурация
```bash
# Использование другого файла конфигурации
python src/main.py --config /path/to/config.ini status

# Подробный вывод (для отладки)
python src/main.py --verbose migrate
```

### Справка
```bash
# Общая справка
python src/main.py --help

# Справка по конкретной команде
python src/main.py migrate --help
python src/main.py migrate-batch --help
python src/main.py migrate-date-range --help
```

## Примеры использования

### Полный цикл миграции

```bash
# 1. Проверяем подключение к БД
python src/main.py test-connection

# 2. Смотрим текущий статус
python src/main.py status

# 3. Мигрируем файлы батчами для контроля
python src/main.py migrate-batch --batch-size 100

# 4. Проверяем прогресс
python src/main.py status

# 5. Мигрируем оставшиеся файлы
python src/main.py migrate

# 6. Проверяем корректность
python src/main.py verify --sample-size 200

# 7. Очищаем ресурсы
python src/main.py cleanup
```

### Миграция по расписанию

```bash
# Миграция файлов за последние 7 дней
python src/main.py migrate-date-range \
  --start-date $(date -d "7 days ago" +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d)

# Миграция файлов за определенный месяц
python src/main.py migrate-date-range \
  --start-date 2024-01-01 \
  --end-date 2024-01-31
```

### Мониторинг процесса

```bash
# Просмотр списка не перемещенных файлов
python src/main.py list-files --type unmoved --limit 50

# Проверка статуса
python src/main.py status

# Проверка корректности на большой выборке
python src/main.py verify --sample-size 500
```

### Отладка и диагностика

```bash
# Подробный вывод для отладки
python src/main.py --verbose migrate-batch --batch-size 10

# Проверка подключения к БД
python src/main.py test-connection

# Просмотр списка файлов для анализа
python src/main.py list-files --type unmoved --limit 100
```

## Коды возврата

CLI использует стандартные коды возврата:

- **0** - Успешное выполнение
- **1** - Ошибка выполнения

Примеры использования в скриптах:

```bash
#!/bin/bash

# Проверяем подключение к БД
if python src/main.py test-connection; then
    echo "✅ Подключение к БД успешно"
    
    # Мигрируем файлы
    if python src/main.py migrate; then
        echo "✅ Миграция завершена успешно"
        
        # Проверяем результат
        python src/main.py verify --sample-size 100
    else
        echo "❌ Ошибка миграции"
        exit 1
    fi
else
    echo "❌ Ошибка подключения к БД"
    exit 1
fi
```

## Обработка ошибок

### Типичные ошибки и решения

#### Ошибка подключения к БД
```bash
❌ Ошибка инициализации: 'Logger' object has no attribute 'log_database_connected'
```
**Решение**: Проверьте конфигурацию БД в `config/settings.ini`

#### Ошибка конфигурации
```bash
❌ Ошибка инициализации: Файл конфигурации не найден: config/settings.ini
```
**Решение**: Убедитесь, что файл конфигурации существует или укажите правильный путь:
```bash
python src/main.py --config /path/to/config.ini status
```

#### Ошибка формата даты
```bash
❌ Ошибка формата даты: time data 'invalid-date' does not match format '%Y-%m-%d'
```
**Решение**: Используйте правильный формат даты YYYY-MM-DD:
```bash
python src/main.py migrate-date-range --start-date 2024-01-01 --end-date 2024-01-31
```

#### Ошибка доступа к файлам
```bash
❌ Ошибка миграции: [Errno 13] Permission denied: 'test_files/file001'
```
**Решение**: Проверьте права доступа к файлам и каталогам

### Отладка

Для получения подробной информации об ошибках используйте флаг `--verbose`:

```bash
python src/main.py --verbose migrate
```

Это выведет полный стек вызовов при возникновении ошибки.

## Автоматизация

### Скрипт для ежедневной миграции

```bash
#!/bin/bash
# daily_migration.sh

LOG_FILE="/var/log/file_migration.log"
DATE=$(date +%Y-%m-%d)

echo "[$DATE] Начало ежедневной миграции" >> $LOG_FILE

# Проверяем подключение к БД
if ! python src/main.py test-connection >> $LOG_FILE 2>&1; then
    echo "[$DATE] ОШИБКА: Не удалось подключиться к БД" >> $LOG_FILE
    exit 1
fi

# Мигрируем файлы за вчерашний день
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
if python src/main.py migrate-date-range --start-date $YESTERDAY --end-date $YESTERDAY >> $LOG_FILE 2>&1; then
    echo "[$DATE] Успешно мигрированы файлы за $YESTERDAY" >> $LOG_FILE
    
    # Проверяем корректность
    python src/main.py verify --sample-size 50 >> $LOG_FILE 2>&1
    
    # Очищаем ресурсы
    python src/main.py cleanup >> $LOG_FILE 2>&1
else
    echo "[$DATE] ОШИБКА: Не удалось мигрировать файлы за $YESTERDAY" >> $LOG_FILE
    exit 1
fi

echo "[$DATE] Ежедневная миграция завершена" >> $LOG_FILE
```

### Мониторинг через cron

```bash
# Добавляем в crontab для ежедневного выполнения в 2:00
0 2 * * * /path/to/daily_migration.sh

# Или для выполнения каждые 4 часа
0 */4 * * * /path/to/migrate_batch.sh
```

### Скрипт для миграции батчами

```bash
#!/bin/bash
# migrate_batch.sh

BATCH_SIZE=100
MAX_ITERATIONS=10

for i in $(seq 1 $MAX_ITERATIONS); do
    echo "Итерация $i/$MAX_ITERATIONS"
    
    if python src/main.py migrate-batch --batch-size $BATCH_SIZE; then
        echo "Батч $i выполнен успешно"
        
        # Проверяем, есть ли еще файлы для миграции
        if python src/main.py status | grep -q "Не перемещено: 0"; then
            echo "Все файлы мигрированы"
            break
        fi
    else
        echo "Ошибка в батче $i"
        exit 1
    fi
    
    # Пауза между батчами
    sleep 5
done
```

## Интеграция с системами мониторинга

### Проверка статуса для мониторинга

```bash
#!/bin/bash
# health_check.sh

# Проверяем подключение к БД
if ! python src/main.py test-connection > /dev/null 2>&1; then
    echo "CRITICAL: Database connection failed"
    exit 2
fi

# Проверяем статус миграции
STATUS_OUTPUT=$(python src/main.py status)
UNMOVED_FILES=$(echo "$STATUS_OUTPUT" | grep "Не перемещено:" | awk '{print $2}')

if [ "$UNMOVED_FILES" -gt 1000 ]; then
    echo "WARNING: Too many unmoved files: $UNMOVED_FILES"
    exit 1
elif [ "$UNMOVED_FILES" -gt 100 ]; then
    echo "WARNING: Some unmoved files: $UNMOVED_FILES"
    exit 1
else
    echo "OK: Migration status normal"
    exit 0
fi
```

### Отправка уведомлений

```bash
#!/bin/bash
# notify_migration.sh

# Выполняем миграцию
if python src/main.py migrate; then
    # Успешная миграция
    MESSAGE="✅ Миграция файлов завершена успешно"
    python src/main.py status | mail -s "Migration Success" admin@company.com
else
    # Ошибка миграции
    MESSAGE="❌ Ошибка миграции файлов"
    python src/main.py status | mail -s "Migration Error" admin@company.com
fi
```

## Производительность

### Оптимизация параметров

```bash
# Для больших объемов данных - увеличиваем размер батча
python src/main.py migrate-batch --batch-size 1000

# Для стабильности - уменьшаем размер батча
python src/main.py migrate-batch --batch-size 50

# Для экономии ресурсов - мигрируем по датам
python src/main.py migrate-date-range --start-date 2024-01-01 --end-date 2024-01-31
```

### Мониторинг производительности

```bash
# Засекаем время выполнения
time python src/main.py migrate

# Проверяем статус после миграции
python src/main.py status
```

## Безопасность

### Проверка перед миграцией

```bash
# Всегда проверяем подключение к БД
python src/main.py test-connection

# Проверяем текущий статус
python src/main.py status

# Тестируем на небольшом батче
python src/main.py migrate-batch --batch-size 10

# Проверяем результат
python src/main.py verify --sample-size 20
```

### Резервное копирование

```bash
# Создаем резервную копию БД перед миграцией
mysqldump -u username -p database_name > backup_$(date +%Y%m%d_%H%M%S).sql

# Выполняем миграцию
python src/main.py migrate

# Проверяем результат
python src/main.py verify --sample-size 100
```

## Расширенное использование

### Создание собственных скриптов

```python
#!/usr/bin/env python3
# custom_migration.py

import subprocess
import sys
from datetime import datetime, timedelta

def run_command(cmd):
    """Выполняет команду CLI и возвращает результат."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr

def main():
    # Проверяем подключение к БД
    success, stdout, stderr = run_command("python src/main.py test-connection")
    if not success:
        print(f"❌ Ошибка подключения к БД: {stderr}")
        return 1
    
    # Мигрируем файлы за последние 3 дня
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3)
    
    cmd = f"python src/main.py migrate-date-range --start-date {start_date.strftime('%Y-%m-%d')} --end-date {end_date.strftime('%Y-%m-%d')}"
    success, stdout, stderr = run_command(cmd)
    
    if success:
        print("✅ Миграция завершена успешно")
        print(stdout)
        return 0
    else:
        print(f"❌ Ошибка миграции: {stderr}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
```

Этот скрипт можно использовать для автоматизации сложных сценариев миграции.
