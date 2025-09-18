"""
Главный модуль CLI интерфейса для утилиты миграции файлов.

Предоставляет командный интерфейс для выполнения миграции файлов,
просмотра статистики и управления процессом.
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from .config_loader import load_config
    from .logger import FileMigratorLogger, setup_logger
    from .migrator import Migrator, create_migrator, MigrationError
    from .db import Database
    from .file_ops import FileOps
except ImportError:
    from config_loader import load_config
    from logger import FileMigratorLogger, setup_logger
    from migrator import Migrator, create_migrator, MigrationError
    from db import Database
    from file_ops import FileOps


class FileMigratorCLI:
    """Класс для обработки команд CLI."""
    
    def __init__(self):
        self.config = None
        self.logger = None
        self.migrator = None
    
    def setup(self, config_path: str = "config/settings.ini") -> bool:
        """
        Инициализирует CLI с конфигурацией.
        
        Args:
            config_path: Путь к файлу конфигурации
            
        Returns:
            bool: True если инициализация успешна
        """
        try:
            # Загружаем конфигурацию
            self.config = load_config(config_path)
            
            # Настраиваем логгер
            self.logger = FileMigratorLogger(self.config.logging)
            
            # Создаем мигратор
            self.migrator = create_migrator(self.config, self.logger)
            
            self.logger.log_system_info(f"Конфигурация загружена из: {config_path}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка инициализации: {e}")
            return False
    
    def cmd_migrate(self, args) -> int:
        """
        Команда миграции файлов.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            # Получаем начальную статистику
            initial_status = self.migrator.get_migration_status()
            total_files = initial_status['database']['unmoved_files']
            
            if total_files == 0:
                print("✅ Нет файлов для миграции")
                return 0
            
            print(f"🚀 Начало миграции {total_files} файлов")
            
            # Выполняем миграцию
            if args.max_files:
                stats = self.migrator.migrate_all(max_files=args.max_files)
            else:
                stats = self.migrator.migrate_all()
            
            # Выводим результаты
            print(f"\n✅ Миграция завершена!")
            print(f"📊 Статистика:")
            print(f"   • Обработано: {stats.processed_files}")
            print(f"   • Успешно: {stats.successful_files}")
            print(f"   • Ошибок: {stats.failed_files}")
            print(f"   • Процент успеха: {stats.get_success_rate():.1f}%")
            print(f"   • Продолжительность: {stats.get_duration():.2f} сек")
            
            if stats.failed_files > 0:
                print(f"\n⚠️ Обнаружено {stats.failed_files} ошибок:")
                for error in stats.errors[:10]:  # Показываем первые 10 ошибок
                    print(f"   • {error['file_id']}: {error['error']}")
                if len(stats.errors) > 10:
                    print(f"   ... и еще {len(stats.errors) - 10} ошибок")
            
            return 0 if stats.failed_files == 0 else 1
            
        except MigrationError as e:
            print(f"❌ Ошибка миграции: {e}")
            return 1
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()
    
    def cmd_migrate_batch(self, args) -> int:
        """
        Команда миграции одного батча.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            batch_size = args.batch_size or self.config.migrator.batch_size
            print(f"🔄 Миграция батча размером {batch_size}")
            
            processed, successful, failed = self.migrator.migrate_batch(batch_size)
            
            print(f"📊 Результат батча:")
            print(f"   • Обработано: {processed}")
            print(f"   • Успешно: {successful}")
            print(f"   • Ошибок: {failed}")
            
            if processed == 0:
                print("ℹ️ Нет файлов для миграции")
            
            return 0 if failed == 0 else 1
            
        except Exception as e:
            print(f"❌ Ошибка миграции батча: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()
    
    def cmd_migrate_date_range(self, args) -> int:
        """
        Команда миграции по диапазону дат.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            # Парсим даты
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
            
            if start_date > end_date:
                print("❌ Начальная дата не может быть больше конечной")
                return 1
            
            print(f"📅 Миграция файлов за период: {start_date.date()} - {end_date.date()}")
            
            stats = self.migrator.migrate_by_date_range(start_date, end_date)
            
            print(f"\n✅ Миграция по диапазону дат завершена!")
            print(f"📊 Статистика:")
            print(f"   • Обработано: {stats.processed_files}")
            print(f"   • Успешно: {stats.successful_files}")
            print(f"   • Ошибок: {stats.failed_files}")
            print(f"   • Продолжительность: {stats.get_duration():.2f} сек")
            
            return 0 if stats.failed_files == 0 else 1
            
        except ValueError as e:
            print(f"❌ Ошибка формата даты: {e}")
            print("Используйте формат YYYY-MM-DD")
            return 1
        except Exception as e:
            print(f"❌ Ошибка миграции по диапазону дат: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()
    
    def cmd_status(self, args) -> int:
        """
        Команда просмотра статуса миграции.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            status = self.migrator.get_migration_status()
            
            print("📊 Статус миграции файлов")
            print("=" * 50)
            
            # Статистика БД
            db_stats = status['database']
            print(f"\n🗄️ База данных:")
            print(f"   • Всего файлов: {db_stats.get('total_files', 0)}")
            print(f"   • Перемещено: {db_stats.get('moved_files', 0)}")
            print(f"   • Не перемещено: {db_stats.get('unmoved_files', 0)}")
            
            if db_stats.get('earliest_file_date'):
                print(f"   • Первый файл: {db_stats['earliest_file_date']}")
            if db_stats.get('latest_file_date'):
                print(f"   • Последний файл: {db_stats['latest_file_date']}")
            
            # Статистика файловой системы
            fs_stats = status['filesystem']
            print(f"\n📁 Файловая система:")
            print(f"   • Не перемещенных файлов: {fs_stats.get('unmoved_files_count', 0)}")
            print(f"   • Размер не перемещенных: {fs_stats.get('unmoved_files_size', 0):,} байт")
            print(f"   • Перемещенных файлов: {fs_stats.get('moved_files_count', 0)}")
            print(f"   • Размер перемещенных: {fs_stats.get('moved_files_size', 0):,} байт")
            print(f"   • Каталогов по датам: {fs_stats.get('date_directories_count', 0)}")
            
            # Прогресс миграции
            total_files = db_stats.get('total_files', 0)
            moved_files = db_stats.get('moved_files', 0)
            if total_files > 0:
                progress = (moved_files / total_files) * 100
                print(f"\n📈 Прогресс миграции:")
                print(f"   • Завершено: {progress:.1f}%")
                print(f"   • Осталось: {total_files - moved_files} файлов")
            
            # Время последнего обновления
            print(f"\n⏰ Время: {status['timestamp']}")
            
            return 0
            
        except Exception as e:
            print(f"❌ Ошибка получения статуса: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()
    
    def cmd_verify(self, args) -> int:
        """
        Команда проверки корректности миграции.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            sample_size = args.sample_size or 100
            print(f"🔍 Проверка корректности миграции на выборке {sample_size} файлов...")
            
            verification_result = self.migrator.verify_migration(sample_size)
            
            print(f"\n📋 Отчет о проверке:")
            print(f"   • Проверено файлов: {verification_result['total_checked']}")
            print(f"   • Корректных: {verification_result['verified']}")
            print(f"   • Ошибок: {verification_result['errors']}")
            
            if verification_result['errors'] > 0:
                print(f"\n⚠️ Обнаружены проблемы:")
                for detail in verification_result['details']:
                    print(f"   • {detail}")
                return 1
            else:
                print("✅ Все проверенные файлы корректны!")
                return 0
            
        except Exception as e:
            print(f"❌ Ошибка проверки: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()
    
    def cmd_cleanup(self, args) -> int:
        """
        Команда очистки ресурсов.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            print("🧹 Выполнение очистки ресурсов...")
            
            # Очищаем пустые каталоги
            removed_dirs = self.migrator.file_ops.cleanup_empty_directories()
            print(f"   • Удалено пустых каталогов: {removed_dirs}")
            
            # Получаем статистику после очистки
            stats = self.migrator.file_ops.get_storage_statistics()
            print(f"   • Каталогов по датам: {stats.get('date_directories_count', 0)}")
            
            print("✅ Очистка завершена")
            return 0
            
        except Exception as e:
            print(f"❌ Ошибка очистки: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()
    
    def cmd_test_connection(self, args) -> int:
        """
        Команда тестирования подключения к БД.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            # Создаем подключение к БД
            db = Database(self.config.database, self.logger)
            
            print("🔌 Тестирование подключения к базе данных...")
            
            if db.test_connection():
                print("✅ Подключение к БД успешно!")
                
                # Получаем информацию о БД
                stats = db.get_migration_statistics()
                print(f"📊 Информация о БД:")
                print(f"   • Всего файлов: {stats.get('total_files', 0)}")
                print(f"   • Перемещено: {stats.get('moved_files', 0)}")
                print(f"   • Не перемещено: {stats.get('unmoved_files', 0)}")
                
                return 0
            else:
                print("❌ Не удалось подключиться к БД")
                return 1
                
        except Exception as e:
            print(f"❌ Ошибка подключения к БД: {e}")
            return 1
        finally:
            if 'db' in locals():
                db.close()
    
    def cmd_list_files(self, args) -> int:
        """
        Команда просмотра списка файлов.
        
        Args:
            args: Аргументы командной строки
            
        Returns:
            int: Код возврата (0 - успех, 1 - ошибка)
        """
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            limit = args.limit or 20
            
            if args.type == 'unmoved':
                print(f"📁 Список не перемещенных файлов (первые {limit}):")
                files = self.migrator.file_ops.list_unmoved_files()
                for i, file_path in enumerate(files[:limit]):
                    size = file_path.stat().st_size
                    print(f"   {i+1:2d}. {file_path.name} ({size:,} байт)")
                
                if len(files) > limit:
                    print(f"   ... и еще {len(files) - limit} файлов")
                    
            elif args.type == 'moved':
                print(f"📁 Список перемещенных файлов (первые {limit}):")
                # Получаем файлы из БД
                with Database(self.config.database, self.logger) as db:
                    moved_files = db.get_files_to_move(limit * 2)  # Получаем больше для фильтрации
                    moved_files = [f for f in moved_files if f['ismooved']]
                    
                    for i, file_info in enumerate(moved_files[:limit]):
                        print(f"   {i+1:2d}. {file_info['IDFL']} - {file_info['filename']} ({file_info['dt']})")
                    
                    if len(moved_files) > limit:
                        print(f"   ... и еще {len(moved_files) - limit} файлов")
            
            return 0
            
        except Exception as e:
            print(f"❌ Ошибка получения списка файлов: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()


def create_parser() -> argparse.ArgumentParser:
    """
    Создает парсер аргументов командной строки.
    
    Returns:
        argparse.ArgumentParser: Настроенный парсер
    """
    parser = argparse.ArgumentParser(
        description="Утилита миграции файлов в структуру по датам",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Миграция всех файлов
  python main.py migrate

  # Миграция с ограничением количества
  python main.py migrate --max-files 1000

  # Миграция одного батча
  python main.py migrate-batch --batch-size 50

  # Миграция файлов за определенный период
  python main.py migrate-date-range --start-date 2024-01-01 --end-date 2024-01-31

  # Просмотр статуса
  python main.py status

  # Проверка корректности миграции
  python main.py verify --sample-size 200

  # Очистка ресурсов
  python main.py cleanup

  # Тестирование подключения к БД
  python main.py test-connection

  # Просмотр списка файлов
  python main.py list-files --type unmoved --limit 10
        """
    )
    
    # Общие аргументы
    parser.add_argument(
        '--config',
        default='config/settings.ini',
        help='Путь к файлу конфигурации (по умолчанию: config/settings.ini)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Подробный вывод'
    )
    
    # Подкоманды
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда migrate
    migrate_parser = subparsers.add_parser('migrate', help='Миграция всех файлов')
    migrate_parser.add_argument(
        '--max-files',
        type=int,
        help='Максимальное количество файлов для миграции'
    )
    
    # Команда migrate-batch
    batch_parser = subparsers.add_parser('migrate-batch', help='Миграция одного батча')
    batch_parser.add_argument(
        '--batch-size',
        type=int,
        help='Размер батча (по умолчанию из конфигурации)'
    )
    
    # Команда migrate-date-range
    date_range_parser = subparsers.add_parser('migrate-date-range', help='Миграция по диапазону дат')
    date_range_parser.add_argument(
        '--start-date',
        required=True,
        help='Начальная дата (формат: YYYY-MM-DD)'
    )
    date_range_parser.add_argument(
        '--end-date',
        required=True,
        help='Конечная дата (формат: YYYY-MM-DD)'
    )
    
    # Команда status
    subparsers.add_parser('status', help='Просмотр статуса миграции')
    
    # Команда verify
    verify_parser = subparsers.add_parser('verify', help='Проверка корректности миграции')
    verify_parser.add_argument(
        '--sample-size',
        type=int,
        default=100,
        help='Размер выборки для проверки (по умолчанию: 100)'
    )
    
    # Команда cleanup
    subparsers.add_parser('cleanup', help='Очистка ресурсов')
    
    # Команда test-connection
    subparsers.add_parser('test-connection', help='Тестирование подключения к БД')
    
    # Команда list-files
    list_parser = subparsers.add_parser('list-files', help='Просмотр списка файлов')
    list_parser.add_argument(
        '--type',
        choices=['unmoved', 'moved'],
        required=True,
        help='Тип файлов для просмотра'
    )
    list_parser.add_argument(
        '--limit',
        type=int,
        default=20,
        help='Максимальное количество файлов для отображения (по умолчанию: 20)'
    )
    
    return parser


def main():
    """Главная функция CLI."""
    parser = create_parser()
    args = parser.parse_args()
    
    # Проверяем, что команда указана
    if not args.command:
        parser.print_help()
        return 1
    
    # Создаем CLI
    cli = FileMigratorCLI()
    
    # Инициализируем CLI
    if not cli.setup(args.config):
        return 1
    
    # Выполняем команду
    try:
        if args.command == 'migrate':
            return cli.cmd_migrate(args)
        elif args.command == 'migrate-batch':
            return cli.cmd_migrate_batch(args)
        elif args.command == 'migrate-date-range':
            return cli.cmd_migrate_date_range(args)
        elif args.command == 'status':
            return cli.cmd_status(args)
        elif args.command == 'verify':
            return cli.cmd_verify(args)
        elif args.command == 'cleanup':
            return cli.cmd_cleanup(args)
        elif args.command == 'test-connection':
            return cli.cmd_test_connection(args)
        elif args.command == 'list-files':
            return cli.cmd_list_files(args)
        else:
            print(f"❌ Неизвестная команда: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️ Операция прервана пользователем")
        return 1
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
