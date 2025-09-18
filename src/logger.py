"""
Модуль для настройки и управления логированием приложения.

Обеспечивает централизованную настройку логирования с ротацией файлов,
цветным выводом в консоль и различными уровнями детализации.
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime

try:
    from .config_loader import LoggingConfig
except ImportError:
    from config_loader import LoggingConfig


class ColoredFormatter(logging.Formatter):
    """Форматтер с цветным выводом для консоли."""
    
    # Цветовые коды ANSI
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        """Форматирует запись лога с цветом."""
        # Добавляем цвет к уровню логирования
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        
        return super().format(record)


class FileMigratorLogger:
    """Класс для управления логированием приложения File Migrator."""
    
    def __init__(self, config: LoggingConfig):
        """
        Инициализация логгера.
        
        Args:
            config: Конфигурация логирования
        """
        self.config = config
        self.logger: Optional[logging.Logger] = None
        self._setup_logger()
    
    def _setup_logger(self) -> None:
        """Настраивает логгер с файловым и консольным выводом."""
        # Создаем логгер
        self.logger = logging.getLogger('file_migrator')
        self.logger.setLevel(getattr(logging, self.config.level.upper()))
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Настраиваем форматтер
        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Настраиваем цветной форматтер для консоли
        colored_formatter = ColoredFormatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Создаем каталог для логов
        log_file_path = Path(self.config.log_file)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Настраиваем файловый обработчик с ротацией
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file_path,
            maxBytes=self.config.max_log_size * 1024 * 1024,  # Конвертируем MB в байты
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, self.config.level.upper()))
        
        # Настраиваем консольный обработчик
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(colored_formatter)
        console_handler.setLevel(getattr(logging, self.config.level.upper()))
        
        # Добавляем обработчики к логгеру
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Предотвращаем дублирование сообщений
        self.logger.propagate = False
    
    def get_logger(self) -> logging.Logger:
        """
        Возвращает настроенный логгер.
        
        Returns:
            logging.Logger: Настроенный логгер
        """
        if self.logger is None:
            raise RuntimeError("Логгер не инициализирован")
        return self.logger
    
    def log_migration_start(self, total_files: int, batch_size: int) -> None:
        """
        Логирует начало процесса миграции.
        
        Args:
            total_files: Общее количество файлов для миграции
            batch_size: Размер батча
        """
        self.logger.info(f"🚀 Начало миграции файлов")
        self.logger.info(f"📊 Всего файлов: {total_files}")
        self.logger.info(f"📦 Размер батча: {batch_size}")
        self.logger.info(f"⏰ Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def log_migration_end(self, processed_files: int, successful_files: int, failed_files: int) -> None:
        """
        Логирует завершение процесса миграции.
        
        Args:
            processed_files: Обработано файлов
            successful_files: Успешно мигрировано
            failed_files: Ошибок при миграции
        """
        self.logger.info(f"✅ Миграция завершена")
        self.logger.info(f"📊 Статистика:")
        self.logger.info(f"   • Обработано: {processed_files}")
        self.logger.info(f"   • Успешно: {successful_files}")
        self.logger.info(f"   • Ошибок: {failed_files}")
        self.logger.info(f"⏰ Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def log_batch_start(self, batch_number: int, batch_size: int) -> None:
        """
        Логирует начало обработки батча.
        
        Args:
            batch_number: Номер батча
            batch_size: Размер батча
        """
        self.logger.info(f"📦 Обработка батча #{batch_number} (размер: {batch_size})")
    
    def log_batch_end(self, batch_number: int, processed: int, successful: int, failed: int) -> None:
        """
        Логирует завершение обработки батча.
        
        Args:
            batch_number: Номер батча
            processed: Обработано файлов
            successful: Успешно мигрировано
            failed: Ошибок
        """
        self.logger.info(f"✅ Батч #{batch_number} завершен: {successful}/{processed} успешно, {failed} ошибок")
    
    def log_file_moved(self, idfl: str, source_path: Path, target_path: Path) -> None:
        """
        Логирует успешное перемещение файла.
        
        Args:
            idfl: Идентификатор файла
            source_path: Исходный путь
            target_path: Целевой путь
        """
        self.logger.info(f"📁 Файл {idfl} перемещен: {source_path} → {target_path}")
    
    def log_file_error(self, idfl: str, error: Exception) -> None:
        """
        Логирует ошибку при обработке файла.
        
        Args:
            idfl: Идентификатор файла
            error: Исключение
        """
        self.logger.error(f"❌ Ошибка при обработке файла {idfl}: {error}")
    
    def log_database_error(self, operation: str, error: Exception) -> None:
        """
        Логирует ошибку базы данных.
        
        Args:
            operation: Операция, при которой произошла ошибка
            error: Исключение
        """
        self.logger.error(f"🗄️ Ошибка БД при {operation}: {error}")
    
    def log_progress(self, current: int, total: int, percentage: float = None) -> None:
        """
        Логирует прогресс выполнения.
        
        Args:
            current: Текущий прогресс
            total: Общее количество
            percentage: Процент выполнения (опционально)
        """
        if percentage is None:
            percentage = (current / total) * 100 if total > 0 else 0
        
        self.logger.info(f"📈 Прогресс: {current}/{total} ({percentage:.1f}%)")
    
    def log_config_loaded(self, config_path: str) -> None:
        """
        Логирует успешную загрузку конфигурации.
        
        Args:
            config_path: Путь к файлу конфигурации
        """
        self.logger.info(f"⚙️ Конфигурация загружена из {config_path}")
    
    def log_database_connected(self, database: str, host: str, port: int) -> None:
        """
        Логирует успешное подключение к базе данных.
        
        Args:
            database: Имя базы данных
            host: Хост БД
            port: Порт БД
        """
        self.logger.info(f"🗄️ Подключение к БД установлено: {database}@{host}:{port}")
    
    def log_database_disconnected(self) -> None:
        """Логирует отключение от базы данных."""
        self.logger.info("🗄️ Подключение к БД закрыто")
    
    def log_file_operation(self, operation: str, file_path: Path, success: bool = True) -> None:
        """
        Логирует операцию с файлом.
        
        Args:
            operation: Тип операции (read, write, move, delete)
            file_path: Путь к файлу
            success: Успешность операции
        """
        status = "✅" if success else "❌"
        self.logger.info(f"{status} {operation.upper()}: {file_path}")
    
    def log_system_info(self, info: str) -> None:
        """
        Логирует системную информацию.
        
        Args:
            info: Информационное сообщение
        """
        self.logger.info(f"ℹ️ {info}")
    
    def log_warning(self, message: str) -> None:
        """
        Логирует предупреждение.
        
        Args:
            message: Сообщение предупреждения
        """
        self.logger.warning(f"⚠️ {message}")
    
    def log_critical_error(self, message: str, error: Exception = None) -> None:
        """
        Логирует критическую ошибку.
        
        Args:
            message: Сообщение об ошибке
            error: Исключение (опционально)
        """
        if error:
            self.logger.critical(f"💥 {message}: {error}")
        else:
            self.logger.critical(f"💥 {message}")


def setup_logger(config: LoggingConfig) -> logging.Logger:
    """
    Удобная функция для быстрой настройки логгера.
    
    Args:
        config: Конфигурация логирования
        
    Returns:
        logging.Logger: Настроенный логгер
    """
    migrator_logger = FileMigratorLogger(config)
    return migrator_logger.get_logger()


def get_logger(name: str = 'file_migrator') -> logging.Logger:
    """
    Получает логгер по имени.
    
    Args:
        name: Имя логгера
        
    Returns:
        logging.Logger: Логгер
    """
    return logging.getLogger(name)


if __name__ == "__main__":
    # Тестирование модуля
    try:
        from .config_loader import load_config
    except ImportError:
        from config_loader import load_config
    
    try:
        # Загружаем конфигурацию
        config = load_config()
        
        # Настраиваем логгер
        logger = setup_logger(config.logging)
        
        # Тестируем различные уровни логирования
        logger.debug("🔍 Отладочное сообщение")
        logger.info("ℹ️ Информационное сообщение")
        logger.warning("⚠️ Предупреждение")
        logger.error("❌ Ошибка")
        logger.critical("💥 Критическая ошибка")
        
        # Тестируем специальные методы
        migrator_logger = FileMigratorLogger(config.logging)
        migrator_logger.log_migration_start(1000, 100)
        migrator_logger.log_file_moved("test001", Path("old/path"), Path("new/path"))
        migrator_logger.log_progress(50, 100)
        migrator_logger.log_migration_end(100, 95, 5)
        
        print("✅ Логгер успешно настроен и протестирован!")
        
    except Exception as e:
        print(f"❌ Ошибка настройки логгера: {e}")
