"""
Модуль бизнес-логики миграции файлов.

Объединяет работу с базой данных и файловой системой для выполнения
миграции файлов из плоской структуры в структуру по датам (YYYYMMDD).
"""

import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from pathlib import Path

try:
    from .config_loader import Config, MigratorConfig
    from .logger import FileMigratorLogger
    from .db import Database
    from .file_ops import FileOps
except ImportError:
    from config_loader import Config, MigratorConfig
    from logger import FileMigratorLogger
    from db import Database
    from file_ops import FileOps


class MigrationError(Exception):
    """Исключение для ошибок миграции."""
    pass


class MigrationStats:
    """Класс для хранения статистики миграции."""
    
    def __init__(self):
        self.total_files = 0
        self.processed_files = 0
        self.successful_files = 0
        self.failed_files = 0
        self.skipped_files = 0
        self.start_time = None
        self.end_time = None
        self.batch_count = 0
        self.errors = []
    
    def add_error(self, file_id: str, error: Exception):
        """Добавляет ошибку в список."""
        self.errors.append({
            'file_id': file_id,
            'error': str(error),
            'timestamp': datetime.now()
        })
    
    def get_duration(self) -> Optional[float]:
        """Возвращает продолжительность миграции в секундах."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def get_success_rate(self) -> float:
        """Возвращает процент успешных миграций."""
        if self.processed_files == 0:
            return 0.0
        return (self.successful_files / self.processed_files) * 100
    
    def to_dict(self) -> Dict:
        """Преобразует статистику в словарь."""
        return {
            'total_files': self.total_files,
            'processed_files': self.processed_files,
            'successful_files': self.successful_files,
            'failed_files': self.failed_files,
            'skipped_files': self.skipped_files,
            'batch_count': self.batch_count,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.get_duration(),
            'success_rate': self.get_success_rate(),
            'error_count': len(self.errors)
        }


class Migrator:
    """Основной класс для миграции файлов."""
    
    def __init__(self, config: Config, logger: FileMigratorLogger):
        """
        Инициализация мигратора.
        
        Args:
            config: Конфигурация приложения
            logger: Логгер для записи операций
        """
        self.config = config
        self.logger = logger
        self.db = Database(config.database, logger)
        self.file_ops = FileOps(config.paths, logger)
        self.stats = MigrationStats()
    
    def initialize(self) -> bool:
        """
        Инициализирует мигратор и проверяет готовность к работе.
        
        Returns:
            bool: True если инициализация успешна
        """
        try:
            self.logger.log_system_info("Инициализация мигратора")
            
            # Проверяем подключение к БД
            if not self.db.test_connection():
                self.logger.log_critical_error("Не удалось подключиться к базе данных")
                return False
            
            # Получаем статистику БД
            db_stats = self.db.get_migration_statistics()
            self.stats.total_files = db_stats.get('unmoved_files', 0)
            
            # Получаем статистику файловой системы
            fs_stats = self.file_ops.get_storage_statistics()
            
            self.logger.log_system_info(f"Готовность к миграции:")
            self.logger.log_system_info(f"  • Файлов в БД: {db_stats.get('total_files', 0)}")
            self.logger.log_system_info(f"  • Не перемещенных: {self.stats.total_files}")
            self.logger.log_system_info(f"  • Файлов в ФС: {fs_stats.get('unmoved_files_count', 0)}")
            
            return True
            
        except Exception as e:
            self.logger.log_critical_error("Ошибка инициализации мигратора", e)
            return False
    
    def migrate_batch(self, batch_size: Optional[int] = None) -> Tuple[int, int, int]:
        """
        Мигрирует один батч файлов.
        
        Args:
            batch_size: Размер батча (по умолчанию из конфигурации)
            
        Returns:
            Tuple[int, int, int]: (обработано, успешно, ошибок)
        """
        if batch_size is None:
            batch_size = self.config.migrator.batch_size
        
        self.stats.batch_count += 1
        batch_number = self.stats.batch_count
        
        self.logger.log_batch_start(batch_number, batch_size)
        
        try:
            # Получаем файлы для миграции
            files = self.db.get_files_to_move(batch_size)
            
            if not files:
                self.logger.log_system_info("Нет файлов для миграции")
                return 0, 0, 0
            
            processed = 0
            successful = 0
            failed = 0
            
            for file_info in files:
                try:
                    result = self._migrate_single_file(file_info)
                    if result:
                        successful += 1
                    else:
                        failed += 1
                    processed += 1
                    
                except Exception as e:
                    failed += 1
                    self.stats.add_error(file_info['IDFL'], e)
                    self.logger.log_file_error(file_info['IDFL'], e)
            
            self.stats.processed_files += processed
            self.stats.successful_files += successful
            self.stats.failed_files += failed
            
            self.logger.log_batch_end(batch_number, processed, successful, failed)
            
            return processed, successful, failed
            
        except Exception as e:
            self.logger.log_database_error("migrate_batch", e)
            raise MigrationError(f"Ошибка миграции батча: {e}")
    
    def _migrate_single_file(self, file_info: Dict) -> bool:
        """
        Мигрирует один файл.
        
        Args:
            file_info: Информация о файле из БД
            
        Returns:
            bool: True если миграция успешна
        """
        idfl = file_info['IDFL']
        dt = file_info['dt']
        filename = file_info['filename']
        
        try:
            # Проверяем существование файла в старой структуре
            if not self.file_ops.file_exists(idfl, ismooved=False, dt=dt):
                self.logger.log_file_error(idfl, FileNotFoundError("Файл не найден в старой структуре"))
                return False
            
            # Получаем хеш файла для проверки целостности
            old_hash = self.file_ops.get_file_hash(idfl, ismooved=False, dt=dt, algorithm="md5")
            
            # Перемещаем файл
            old_path = self.file_ops.base_path / idfl
            new_path = self.file_ops.move_file(idfl, dt)
            
            # Проверяем целостность после перемещения
            new_hash = self.file_ops.get_file_hash(idfl, ismooved=True, dt=dt, algorithm="md5")
            
            if old_hash and new_hash and old_hash != new_hash:
                self.logger.log_file_error(idfl, ValueError("Ошибка целостности файла после перемещения"))
                return False
            
            # Отмечаем файл как перемещенный в БД
            if not self.db.mark_file_moved(idfl, datetime.now()):
                self.logger.log_database_error("mark_file_moved", Exception(f"Не удалось отметить файл {idfl} как перемещенный"))
                return False
            
            self.logger.log_file_moved(idfl, old_path, new_path)
            return True
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            return False
    
    def migrate_all(self, max_files: Optional[int] = None) -> MigrationStats:
        """
        Мигрирует все файлы.
        
        Args:
            max_files: Максимальное количество файлов для миграции
            
        Returns:
            MigrationStats: Статистика миграции
        """
        self.stats.start_time = datetime.now()
        
        try:
            # Инициализируем мигратор
            if not self.initialize():
                raise MigrationError("Не удалось инициализировать мигратор")
            
            self.logger.log_migration_start(
                total_files=self.stats.total_files,
                batch_size=self.config.migrator.batch_size
            )
            
            # Мигрируем файлы батчами
            while True:
                # Проверяем лимит файлов
                if max_files and self.stats.processed_files >= max_files:
                    self.logger.log_system_info(f"Достигнут лимит файлов: {max_files}")
                    break
                
                # Получаем размер батча с учетом лимита
                batch_size = self.config.migrator.batch_size
                if max_files:
                    remaining = max_files - self.stats.processed_files
                    batch_size = min(batch_size, remaining)
                
                # Мигрируем батч
                processed, successful, failed = self.migrate_batch(batch_size)
                
                # Если нет файлов для обработки, завершаем
                if processed == 0:
                    self.logger.log_system_info("Все файлы обработаны")
                    break
                
                # Логируем прогресс
                progress = (self.stats.processed_files / self.stats.total_files) * 100 if self.stats.total_files > 0 else 0
                self.logger.log_progress(self.stats.processed_files, self.stats.total_files, progress)
                
                # Небольшая пауза между батчами
                if self.config.migrator.retry_delay > 0:
                    time.sleep(self.config.migrator.retry_delay)
            
            self.stats.end_time = datetime.now()
            
            # Финальная статистика
            self.logger.log_migration_end(
                processed_files=self.stats.processed_files,
                successful_files=self.stats.successful_files,
                failed_files=self.stats.failed_files
            )
            
            return self.stats
            
        except Exception as e:
            self.stats.end_time = datetime.now()
            self.logger.log_critical_error("Ошибка миграции", e)
            raise MigrationError(f"Ошибка миграции: {e}")
    
    def migrate_by_date_range(self, start_date: datetime, end_date: datetime) -> MigrationStats:
        """
        Мигрирует файлы в указанном диапазоне дат.
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            
        Returns:
            MigrationStats: Статистика миграции
        """
        self.stats.start_time = datetime.now()
        
        try:
            # Инициализируем мигратор
            if not self.initialize():
                raise MigrationError("Не удалось инициализировать мигратор")
            
            # Получаем файлы в диапазоне дат
            files = self.db.get_files_by_date_range(start_date, end_date)
            unmoved_files = [f for f in files if not f['ismooved']]
            
            self.stats.total_files = len(unmoved_files)
            
            self.logger.log_system_info(f"Миграция файлов в диапазоне {start_date.date()} - {end_date.date()}")
            self.logger.log_system_info(f"Найдено файлов: {len(files)}, не перемещенных: {len(unmoved_files)}")
            
            if not unmoved_files:
                self.logger.log_system_info("Нет файлов для миграции в указанном диапазоне")
                self.stats.end_time = datetime.now()
                return self.stats
            
            # Мигрируем файлы
            for file_info in unmoved_files:
                try:
                    result = self._migrate_single_file(file_info)
                    if result:
                        self.stats.successful_files += 1
                    else:
                        self.stats.failed_files += 1
                    self.stats.processed_files += 1
                    
                except Exception as e:
                    self.stats.failed_files += 1
                    self.stats.add_error(file_info['IDFL'], e)
                    self.logger.log_file_error(file_info['IDFL'], e)
            
            self.stats.end_time = datetime.now()
            
            self.logger.log_migration_end(
                processed_files=self.stats.processed_files,
                successful_files=self.stats.successful_files,
                failed_files=self.stats.failed_files
            )
            
            return self.stats
            
        except Exception as e:
            self.stats.end_time = datetime.now()
            self.logger.log_critical_error("Ошибка миграции по диапазону дат", e)
            raise MigrationError(f"Ошибка миграции по диапазону дат: {e}")
    
    def verify_migration(self, sample_size: int = 100) -> Dict:
        """
        Проверяет корректность миграции на выборке файлов.
        
        Args:
            sample_size: Размер выборки для проверки
            
        Returns:
            Dict: Результаты проверки
        """
        try:
            self.logger.log_system_info(f"Проверка миграции на выборке {sample_size} файлов")
            
            # Получаем перемещенные файлы
            moved_files = self.db.get_files_to_move(sample_size)
            moved_files = [f for f in moved_files if f['ismooved']]
            
            if not moved_files:
                self.logger.log_system_info("Нет перемещенных файлов для проверки")
                return {'verified': 0, 'errors': 0, 'total_checked': 0, 'details': []}
            
            verified = 0
            errors = 0
            details = []
            
            for file_info in moved_files[:sample_size]:
                idfl = file_info['IDFL']
                dt = file_info['dt']
                
                try:
                    # Проверяем существование файла в новой структуре
                    if not self.file_ops.file_exists(idfl, ismooved=True, dt=dt):
                        errors += 1
                        details.append(f"Файл {idfl} не найден в новой структуре")
                        continue
                    
                    # Проверяем размер файла
                    size = self.file_ops.get_file_size(idfl, ismooved=True, dt=dt)
                    if size is None or size == 0:
                        errors += 1
                        details.append(f"Файл {idfl} имеет нулевой размер")
                        continue
                    
                    verified += 1
                    
                except Exception as e:
                    errors += 1
                    details.append(f"Ошибка проверки файла {idfl}: {e}")
            
            result = {
                'verified': verified,
                'errors': errors,
                'total_checked': len(moved_files[:sample_size]),
                'details': details
            }
            
            self.logger.log_system_info(f"Проверка завершена: {verified} корректных, {errors} ошибок")
            
            return result
            
        except Exception as e:
            self.logger.log_database_error("verify_migration", e)
            raise MigrationError(f"Ошибка проверки миграции: {e}")
    
    def get_migration_status(self) -> Dict:
        """
        Получает текущий статус миграции.
        
        Returns:
            Dict: Статус миграции
        """
        try:
            # Статистика БД
            db_stats = self.db.get_migration_statistics()
            
            # Статистика файловой системы
            fs_stats = self.file_ops.get_storage_statistics()
            
            # Статистика мигратора
            migrator_stats = self.stats.to_dict()
            
            status = {
                'database': db_stats,
                'filesystem': fs_stats,
                'migrator': migrator_stats,
                'timestamp': datetime.now().isoformat()
            }
            
            return status
            
        except Exception as e:
            self.logger.log_database_error("get_migration_status", e)
            raise MigrationError(f"Ошибка получения статуса миграции: {e}")
    
    def cleanup(self) -> None:
        """Очищает ресурсы и закрывает соединения."""
        try:
            self.logger.log_system_info("Очистка ресурсов мигратора")
            
            # Очищаем пустые каталоги
            removed_dirs = self.file_ops.cleanup_empty_directories()
            if removed_dirs > 0:
                self.logger.log_system_info(f"Удалено пустых каталогов: {removed_dirs}")
            
            # Закрываем соединения с БД
            self.db.close()
            
            self.logger.log_system_info("Очистка завершена")
            
        except Exception as e:
            self.logger.log_database_error("cleanup", e)
    
    def __enter__(self):
        """Поддержка контекстного менеджера."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Поддержка контекстного менеджера."""
        self.cleanup()


def create_migrator(config: Config, logger: FileMigratorLogger) -> Migrator:
    """
    Удобная функция для создания объекта мигратора.
    
    Args:
        config: Конфигурация приложения
        logger: Логгер
        
    Returns:
        Migrator: Объект мигратора
    """
    return Migrator(config, logger)


if __name__ == "__main__":
    # Тестирование модуля
    try:
        from config_loader import load_config
        from logger import FileMigratorLogger
        
        # Загружаем конфигурацию
        app_config = load_config()
        
        # Настраиваем логгер
        logger = FileMigratorLogger(app_config.logging)
        
        # Создаем мигратор
        migrator = Migrator(app_config, logger)
        
        print("✅ Migrator успешно инициализирован!")
        
        # Получаем статус миграции
        status = migrator.get_migration_status()
        print(f"📊 Статус миграции:")
        print(f"   • Всего файлов в БД: {status['database'].get('total_files', 0)}")
        print(f"   • Перемещено: {status['database'].get('moved_files', 0)}")
        print(f"   • Не перемещено: {status['database'].get('unmoved_files', 0)}")
        print(f"   • Файлов в ФС: {status['filesystem'].get('unmoved_files_count', 0)}")
        
        # Тестируем миграцию одного батча
        print("\n🔄 Тестирование миграции одного батча...")
        processed, successful, failed = migrator.migrate_batch(5)
        print(f"   • Обработано: {processed}")
        print(f"   • Успешно: {successful}")
        print(f"   • Ошибок: {failed}")
        
        # Очищаем ресурсы
        migrator.cleanup()
        
    except Exception as e:
        print(f"❌ Ошибка тестирования Migrator: {e}")
        import traceback
        traceback.print_exc()
