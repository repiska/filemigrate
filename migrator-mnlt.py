#!/usr/bin/env python3
"""
Монолитная версия утилиты миграции файлов.

Все компоненты объединены в один файл для упрощения развертывания.
Версия: 1.0.0
"""

import argparse
import configparser
import hashlib
import logging
import logging.handlers
import mysql.connector
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Union
from mysql.connector import Error, pooling


# ==================== КОНФИГУРАЦИЯ ====================

@dataclass
class DatabaseConfig:
    """Конфигурация подключения к базе данных."""
    driver: str
    host: str
    port: int
    database: str
    username: str
    password: str
    trusted_connection: bool = False


@dataclass
class PathsConfig:
    """Конфигурация путей к файлам."""
    file_path: Path
    new_file_path: Path


@dataclass
class MigratorConfig:
    """Конфигурация параметров миграции."""
    batch_size: int
    max_retries: int
    retry_delay: float


@dataclass
class LoggingConfig:
    """Конфигурация логирования."""
    level: str
    log_file: Path
    max_log_size: int
    backup_count: int


@dataclass
class Config:
    """Основная конфигурация приложения."""
    database: DatabaseConfig
    paths: PathsConfig
    migrator: MigratorConfig
    logging: LoggingConfig


# ==================== ИСКЛЮЧЕНИЯ ====================

class MigrationError(Exception):
    """Исключение для ошибок миграции."""
    pass


class DatabaseConnectionError(Exception):
    """Исключение для ошибок подключения к БД."""
    pass


class DatabaseQueryError(Exception):
    """Исключение для ошибок выполнения запросов."""
    pass


class FileOperationError(Exception):
    """Исключение для ошибок операций с файлами."""
    pass


class FileNotFoundError(FileOperationError):
    """Исключение для случая, когда файл не найден."""
    pass


# ==================== ЛОГИРОВАНИЕ ====================

class ColoredFormatter(logging.Formatter):
    """Форматтер с цветным выводом для консоли."""
    
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
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)


class FileMigratorLogger:
    """Класс для управления логированием приложения File Migrator."""
    
    def __init__(self, config: LoggingConfig):
        self.config = config
        self.logger: Optional[logging.Logger] = None
        self._setup_logger()
    
    def _setup_logger(self) -> None:
        """Настраивает логгер с файловым и консольным выводом."""
        self.logger = logging.getLogger('file_migrator')
        self.logger.setLevel(getattr(logging, self.config.level.upper()))
        self.logger.handlers.clear()
        
        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        colored_formatter = ColoredFormatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        log_file_path = Path(self.config.log_file)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file_path,
            maxBytes=self.config.max_log_size * 1024 * 1024,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, self.config.level.upper()))
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(colored_formatter)
        console_handler.setLevel(getattr(logging, self.config.level.upper()))
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def get_logger(self) -> logging.Logger:
        if self.logger is None:
            raise RuntimeError("Логгер не инициализирован")
        return self.logger
    
    # Методы логирования
    def log_migration_start(self, total_files: int, batch_size: int) -> None:
        self.logger.info(f"🚀 Начало миграции файлов")
        self.logger.info(f"📊 Всего файлов: {total_files}")
        self.logger.info(f"📦 Размер батча: {batch_size}")
        self.logger.info(f"⏰ Время начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def log_migration_end(self, processed_files: int, successful_files: int, failed_files: int) -> None:
        self.logger.info(f"✅ Миграция завершена")
        self.logger.info(f"📊 Статистика:")
        self.logger.info(f"   • Обработано: {processed_files}")
        self.logger.info(f"   • Успешно: {successful_files}")
        self.logger.info(f"   • Ошибок: {failed_files}")
        self.logger.info(f"⏰ Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def log_batch_start(self, batch_number: int, batch_size: int) -> None:
        self.logger.info(f"📦 Обработка батча #{batch_number} (размер: {batch_size})")
    
    def log_batch_end(self, batch_number: int, processed: int, successful: int, failed: int) -> None:
        self.logger.info(f"✅ Батч #{batch_number} завершен: {successful}/{processed} успешно, {failed} ошибок")
    
    def log_file_moved(self, idfl: str, source_path: Path, target_path: Path) -> None:
        self.logger.info(f"📁 Файл {idfl} перемещен: {source_path} → {target_path}")
    
    def log_file_error(self, idfl: str, error: Exception) -> None:
        self.logger.error(f"❌ Ошибка при обработке файла {idfl}: {error}")
    
    def log_database_error(self, operation: str, error: Exception) -> None:
        self.logger.error(f"🗄️ Ошибка БД при {operation}: {error}")
    
    def log_progress(self, current: int, total: int, percentage: float = None) -> None:
        if percentage is None:
            percentage = (current / total) * 100 if total > 0 else 0
        self.logger.info(f"📈 Прогресс: {current}/{total} ({percentage:.1f}%)")
    
    def log_config_loaded(self, config_path: str) -> None:
        self.logger.info(f"⚙️ Конфигурация загружена из {config_path}")
    
    def log_database_connected(self, database: str, host: str, port: int) -> None:
        self.logger.info(f"🗄️ Подключение к БД установлено: {database}@{host}:{port}")
    
    def log_database_disconnected(self) -> None:
        self.logger.info("🗄️ Подключение к БД закрыто")
    
    def log_file_operation(self, operation: str, file_path: Path, success: bool = True) -> None:
        status = "✅" if success else "❌"
        self.logger.info(f"{status} {operation.upper()}: {file_path}")
    
    def log_system_info(self, info: str) -> None:
        self.logger.info(f"ℹ️ {info}")
    
    def log_warning(self, message: str) -> None:
        self.logger.warning(f"⚠️ {message}")
    
    def log_critical_error(self, message: str, error: Exception = None) -> None:
        if error:
            self.logger.critical(f"💥 {message}: {error}")
        else:
            self.logger.critical(f"💥 {message}")


# ==================== ЗАГРУЗЧИК КОНФИГУРАЦИИ ====================

class ConfigLoader:
    """Класс для загрузки и валидации конфигурации."""
    
    def __init__(self, config_path: str = "config/settings.ini"):
        self.config_path = Path(config_path)
        self._config: Optional[Config] = None
    
    def load_config(self) -> Config:
        """Загружает конфигурацию из файла."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Файл конфигурации не найден: {self.config_path}")
        
        config_parser = configparser.ConfigParser()
        config_parser.read(self.config_path, encoding='utf-8')
        
        try:
            db_config = self._load_database_config(config_parser)
            paths_config = self._load_paths_config(config_parser)
            migrator_config = self._load_migrator_config(config_parser)
            logging_config = self._load_logging_config(config_parser)
            
            self._config = Config(
                database=db_config,
                paths=paths_config,
                migrator=migrator_config,
                logging=logging_config
            )
            
            self._validate_config()
            return self._config
            
        except Exception as e:
            raise ValueError(f"Ошибка загрузки конфигурации: {e}")
    
    def _load_database_config(self, parser: configparser.ConfigParser) -> DatabaseConfig:
        """Загружает конфигурацию базы данных."""
        section = 'database'
        if not parser.has_section(section):
            raise ValueError(f"Секция '{section}' не найдена в конфигурации")
        
        driver = parser.get(section, 'driver', fallback='mysql')
        
        if driver == 'mysql':
            return DatabaseConfig(
                driver=driver,
                host=parser.get(section, 'host'),
                port=parser.getint(section, 'port', fallback=3306),
                database=parser.get(section, 'database'),
                username=parser.get(section, 'username'),
                password=parser.get(section, 'password')
            )
        else:
            raise ValueError(f"Неподдерживаемый драйвер БД: {driver}")
    
    def _load_paths_config(self, parser: configparser.ConfigParser) -> PathsConfig:
        """Загружает конфигурацию путей."""
        section = 'paths'
        if not parser.has_section(section):
            raise ValueError(f"Секция '{section}' не найдена в конфигурации")
        
        return PathsConfig(
            file_path=Path(parser.get(section, 'file_path')),
            new_file_path=Path(parser.get(section, 'new_file_path'))
        )
    
    def _load_migrator_config(self, parser: configparser.ConfigParser) -> MigratorConfig:
        """Загружает конфигурацию мигратора."""
        section = 'migrator'
        if not parser.has_section(section):
            raise ValueError(f"Секция '{section}' не найдена в конфигурации")
        
        return MigratorConfig(
            batch_size=parser.getint(section, 'batch_size', fallback=1000),
            max_retries=parser.getint(section, 'max_retries', fallback=3),
            retry_delay=parser.getfloat(section, 'retry_delay', fallback=1.0)
        )
    
    def _load_logging_config(self, parser: configparser.ConfigParser) -> LoggingConfig:
        """Загружает конфигурацию логирования."""
        section = 'logging'
        if not parser.has_section(section):
            raise ValueError(f"Секция '{section}' не найдена в конфигурации")
        
        return LoggingConfig(
            level=parser.get(section, 'level', fallback='INFO'),
            log_file=Path(parser.get(section, 'log_file', fallback='logs/migrator.log')),
            max_log_size=parser.getint(section, 'max_log_size', fallback=10),
            backup_count=parser.getint(section, 'backup_count', fallback=5)
        )
    
    def _validate_config(self) -> None:
        """Валидирует загруженную конфигурацию."""
        if not self._config:
            raise ValueError("Конфигурация не загружена")
        
        if not self._config.paths.file_path.exists():
            raise ValueError(f"Путь к файлам не существует: {self._config.paths.file_path}")
        
        if self._config.migrator.batch_size <= 0:
            raise ValueError("Размер батча должен быть больше 0")
        
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self._config.logging.level.upper() not in valid_levels:
            raise ValueError(f"Некорректный уровень логирования: {self._config.logging.level}")


def load_config(config_path: str = "config/settings.ini") -> Config:
    """Удобная функция для быстрой загрузки конфигурации."""
    loader = ConfigLoader(config_path)
    return loader.load_config()


# ==================== РАБОТА С БАЗОЙ ДАННЫХ ====================

class Database:
    """Класс для работы с базой данных MySQL."""
    
    def __init__(self, config: DatabaseConfig, logger: FileMigratorLogger):
        self.config = config
        self.logger = logger
        self.connection_pool: Optional[pooling.MySQLConnectionPool] = None
        self._setup_connection_pool()
    
    def _setup_connection_pool(self) -> None:
        """Настраивает пул соединений с базой данных."""
        try:
            pool_config = {
                'pool_name': 'file_migrator_pool',
                'pool_size': 5,
                'pool_reset_session': True,
                'host': self.config.host,
                'port': self.config.port,
                'database': self.config.database,
                'user': self.config.username,
                'password': self.config.password,
                'autocommit': True,
                'charset': 'utf8mb4',
                'collation': 'utf8mb4_unicode_ci',
                'raise_on_warnings': True,
                'use_unicode': True
            }
            
            self.connection_pool = pooling.MySQLConnectionPool(**pool_config)
            self.logger.log_database_connected(
                self.config.database, 
                self.config.host, 
                self.config.port
            )
            
        except Error as e:
            self.logger.log_database_error("pool_setup", e)
            raise DatabaseConnectionError(f"Ошибка создания пула соединений: {e}")
    
    def _get_connection(self) -> mysql.connector.connection.MySQLConnection:
        """Получает соединение из пула."""
        try:
            if self.connection_pool is None:
                raise DatabaseConnectionError("Пул соединений не инициализирован")
            
            connection = self.connection_pool.get_connection()
            return connection
            
        except Error as e:
            self.logger.log_database_error("get_connection", e)
            raise DatabaseConnectionError(f"Ошибка получения соединения: {e}")
    
    def _execute_query(self, query: str, params: Tuple = None, fetch: bool = False) -> Optional[List[Dict]]:
        """Выполняет SQL запрос."""
        connection = None
        cursor = None
        
        try:
            connection = self._get_connection()
            cursor = connection.cursor(dictionary=True)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                result = cursor.fetchall()
                return result
            else:
                connection.commit()
                return None
                
        except Error as e:
            if connection:
                connection.rollback()
            self.logger.log_database_error("execute_query", e)
            raise DatabaseQueryError(f"Ошибка выполнения запроса: {e}")
            
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def test_connection(self) -> bool:
        """Тестирует подключение к базе данных."""
        try:
            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            connection.close()
            
            self.logger.log_system_info("Тест подключения к БД успешен")
            return True
            
        except Exception as e:
            self.logger.log_database_error("test_connection", e)
            return False
    
    def get_files_to_move(self, batch_size: int) -> List[Dict]:
        """Получает список файлов для миграции."""
        query = """
        SELECT IDFL, dt, filename, ismooved, dtmoove, created_at, updated_at
        FROM repl_AV_ATF 
        WHERE ismooved = 0 
        ORDER BY dt ASC, IDFL ASC
        LIMIT %s
        """
        
        try:
            result = self._execute_query(query, (batch_size,), fetch=True)
            self.logger.log_system_info(f"Получено {len(result)} файлов для миграции")
            return result
            
        except DatabaseQueryError as e:
            self.logger.log_database_error("get_files_to_move", e)
            raise
    
    def mark_file_moved(self, idfl: str, dtmoove: datetime = None) -> bool:
        """Отмечает файл как перемещенный."""
        if dtmoove is None:
            dtmoove = datetime.now()
        
        query = """
        UPDATE repl_AV_ATF 
        SET ismooved = 1, dtmoove = %s, updated_at = NOW()
        WHERE IDFL = %s AND ismooved = 0
        """
        
        try:
            self._execute_query(query, (dtmoove, idfl))
            self.logger.log_system_info(f"Файл {idfl} отмечен как перемещенный")
            return True
            
        except DatabaseQueryError as e:
            self.logger.log_database_error("mark_file_moved", e)
            return False
    
    def get_migration_statistics(self) -> Dict:
        """Получает статистику миграции."""
        query = """
        SELECT 
            COUNT(*) as total_files,
            SUM(CASE WHEN ismooved = 1 THEN 1 ELSE 0 END) as moved_files,
            SUM(CASE WHEN ismooved = 0 THEN 1 ELSE 0 END) as unmoved_files,
            MIN(dt) as earliest_file_date,
            MAX(dt) as latest_file_date
        FROM repl_AV_ATF
        """
        
        try:
            result = self._execute_query(query, fetch=True)
            if result:
                stats = result[0]
                self.logger.log_system_info("Получена статистика миграции")
                return stats
            else:
                return {}
                
        except DatabaseQueryError as e:
            self.logger.log_database_error("get_migration_statistics", e)
            raise
    
    def get_files_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Получает файлы в указанном диапазоне дат."""
        query = """
        SELECT IDFL, dt, filename, ismooved, dtmoove, created_at, updated_at
        FROM repl_AV_ATF 
        WHERE dt BETWEEN %s AND %s
        ORDER BY dt ASC, IDFL ASC
        """
        
        try:
            result = self._execute_query(query, (start_date, end_date), fetch=True)
            self.logger.log_system_info(f"Найдено {len(result)} файлов в диапазоне {start_date} - {end_date}")
            return result
            
        except DatabaseQueryError as e:
            self.logger.log_database_error("get_files_by_date_range", e)
            raise
    
    def close(self) -> None:
        """Закрывает все соединения и очищает ресурсы."""
        try:
            if self.connection_pool:
                self.connection_pool = None
            self.logger.log_database_disconnected()
        except Exception as e:
            self.logger.log_database_error("close", e)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# ==================== ОПЕРАЦИИ С ФАЙЛОВОЙ СИСТЕМОЙ ====================

class FileOps:
    """Класс для операций с файловой системой."""
    
    def __init__(self, paths_config: PathsConfig, logger: FileMigratorLogger):
        self.paths_config = paths_config
        self.logger = logger
        self.base_path = Path(paths_config.file_path)
        self.new_base_path = Path(paths_config.new_file_path)
        self._ensure_directories_exist()
    
    def _ensure_directories_exist(self) -> None:
        """Создает необходимые каталоги если они не существуют."""
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            self.new_base_path.mkdir(parents=True, exist_ok=True)
            self.logger.log_system_info(f"Каталоги созданы: {self.base_path}, {self.new_base_path}")
        except Exception as e:
            self.logger.log_database_error("create_directories", e)
            raise FileOperationError(f"Ошибка создания каталогов: {e}")
    
    def _get_date_directory(self, dt: datetime) -> Path:
        """Получает путь к каталогу по дате в формате YYYYMMDD."""
        date_str = dt.strftime("%Y%m%d")
        return self.new_base_path / date_str
    
    def _ensure_date_directory_exists(self, dt: datetime) -> Path:
        """Создает каталог по дате если он не существует."""
        date_dir = self._get_date_directory(dt)
        try:
            date_dir.mkdir(parents=True, exist_ok=True)
            return date_dir
        except Exception as e:
            self.logger.log_database_error("create_date_directory", e)
            raise FileOperationError(f"Ошибка создания каталога по дате: {e}")
    
    def move_file(self, idfl: str, dt: datetime) -> Path:
        """Перемещает файл в новую структуру каталогов по дате."""
        source_path = self.base_path / idfl
        
        if not source_path.exists():
            error_msg = f"Исходный файл не найден: {source_path}"
            self.logger.log_file_error(idfl, FileNotFoundError(error_msg))
            raise FileNotFoundError(error_msg)
        
        try:
            target_dir = self._ensure_date_directory_exists(dt)
            target_path = target_dir / idfl
            
            if target_path.exists():
                target_path = self._get_unique_filename(target_dir, idfl)
            
            shutil.move(str(source_path), str(target_path))
            
            self.logger.log_file_moved(idfl, source_path, target_path)
            self.logger.log_file_operation("move", target_path, True)
            
            return target_path
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            raise FileOperationError(f"Ошибка перемещения файла {idfl}: {e}")
    
    def _get_unique_filename(self, directory: Path, filename: str) -> Path:
        """Получает уникальное имя файла в каталоге."""
        base_path = directory / filename
        if not base_path.exists():
            return base_path
        
        name_parts = filename.rsplit('.', 1)
        if len(name_parts) == 2:
            base_name, extension = name_parts
            extension = '.' + extension
        else:
            base_name = filename
            extension = ''
        
        counter = 1
        while True:
            new_name = f"{base_name}_{counter}{extension}"
            new_path = directory / new_name
            if not new_path.exists():
                return new_path
            counter += 1
    
    def file_exists(self, idfl: str, ismooved: bool, dt: datetime) -> bool:
        """Проверяет существование файла по старой или новой схеме."""
        try:
            if ismooved:
                date_dir = self._get_date_directory(dt)
                file_path = date_dir / idfl
            else:
                file_path = self.base_path / idfl
            
            exists = file_path.exists()
            self.logger.log_system_info(f"Проверка файла {idfl}: {'существует' if exists else 'не найден'}")
            return exists
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            return False
    
    def get_file_size(self, idfl: str, ismooved: bool, dt: datetime) -> Optional[int]:
        """Получает размер файла в байтах."""
        try:
            if ismooved:
                date_dir = self._get_date_directory(dt)
                file_path = date_dir / idfl
            else:
                file_path = self.base_path / idfl
            
            if file_path.exists():
                size = file_path.stat().st_size
                self.logger.log_system_info(f"Размер файла {idfl}: {size} байт")
                return size
            else:
                return None
                
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            return None
    
    def get_file_hash(self, idfl: str, ismooved: bool, dt: datetime, algorithm: str = 'md5') -> Optional[str]:
        """Получает хеш файла для проверки целостности."""
        try:
            if ismooved:
                date_dir = self._get_date_directory(dt)
                file_path = date_dir / idfl
            else:
                file_path = self.base_path / idfl
            
            if not file_path.exists():
                return None
            
            if algorithm == 'md5':
                hasher = hashlib.md5()
            elif algorithm == 'sha1':
                hasher = hashlib.sha1()
            elif algorithm == 'sha256':
                hasher = hashlib.sha256()
            else:
                raise ValueError(f"Неподдерживаемый алгоритм: {algorithm}")
            
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            
            file_hash = hasher.hexdigest()
            self.logger.log_system_info(f"Хеш файла {idfl} ({algorithm}): {file_hash}")
            return file_hash
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            return None
    
    def list_unmoved_files(self) -> List[Path]:
        """Получает список файлов в базовом каталоге (не перемещенных)."""
        try:
            if not self.base_path.exists():
                return []
            
            files = [f for f in self.base_path.iterdir() if f.is_file()]
            self.logger.log_system_info(f"Не перемещенных файлов: {len(files)}")
            return files
            
        except Exception as e:
            self.logger.log_database_error("list_unmoved_files", e)
            return []
    
    def cleanup_empty_directories(self) -> int:
        """Удаляет пустые каталоги в новой структуре."""
        try:
            removed_count = 0
            
            if not self.new_base_path.exists():
                return 0
            
            for date_dir in self.new_base_path.iterdir():
                if date_dir.is_dir():
                    try:
                        if not any(date_dir.iterdir()):
                            date_dir.rmdir()
                            removed_count += 1
                            self.logger.log_system_info(f"Удален пустой каталог: {date_dir}")
                    except OSError:
                        pass
            
            if removed_count > 0:
                self.logger.log_system_info(f"Удалено пустых каталогов: {removed_count}")
            
            return removed_count
            
        except Exception as e:
            self.logger.log_database_error("cleanup_empty_directories", e)
            return 0
    
    def get_storage_statistics(self) -> dict:
        """Получает статистику использования дискового пространства."""
        try:
            stats = {
                'base_path': str(self.base_path),
                'new_base_path': str(self.new_base_path),
                'unmoved_files_count': 0,
                'unmoved_files_size': 0,
                'date_directories_count': 0,
                'moved_files_count': 0,
                'moved_files_size': 0
            }
            
            if self.base_path.exists():
                unmoved_files = self.list_unmoved_files()
                stats['unmoved_files_count'] = len(unmoved_files)
                stats['unmoved_files_size'] = sum(f.stat().st_size for f in unmoved_files)
            
            if self.new_base_path.exists():
                date_dirs = [d for d in self.new_base_path.iterdir() if d.is_dir()]
                stats['date_directories_count'] = len(date_dirs)
                
                total_moved_files = 0
                total_moved_size = 0
                
                for date_dir in date_dirs:
                    files = [f for f in date_dir.iterdir() if f.is_file()]
                    total_moved_files += len(files)
                    total_moved_size += sum(f.stat().st_size for f in files)
                
                stats['moved_files_count'] = total_moved_files
                stats['moved_files_size'] = total_moved_size
            
            self.logger.log_system_info(f"Статистика хранилища: {stats}")
            return stats
            
        except Exception as e:
            self.logger.log_database_error("get_storage_statistics", e)
            return {}


# ==================== СТАТИСТИКА МИГРАЦИИ ====================

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


# ==================== МИГРАТОР ====================

class Migrator:
    """Основной класс для миграции файлов."""
    
    def __init__(self, config: Config, logger: FileMigratorLogger):
        self.config = config
        self.logger = logger
        self.db = Database(config.database, logger)
        self.file_ops = FileOps(config.paths, logger)
        self.stats = MigrationStats()
    
    def initialize(self) -> bool:
        """Инициализирует мигратор и проверяет готовность к работе."""
        try:
            self.logger.log_system_info("Инициализация мигратора")
            
            if not self.db.test_connection():
                self.logger.log_critical_error("Не удалось подключиться к базе данных")
                return False
            
            db_stats = self.db.get_migration_statistics()
            self.stats.total_files = db_stats.get('unmoved_files', 0)
            
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
        """Мигрирует один батч файлов."""
        if batch_size is None:
            batch_size = self.config.migrator.batch_size
        
        self.stats.batch_count += 1
        batch_number = self.stats.batch_count
        
        self.logger.log_batch_start(batch_number, batch_size)
        
        try:
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
        """Мигрирует один файл."""
        idfl = file_info['IDFL']
        dt = file_info['dt']
        
        try:
            if not self.file_ops.file_exists(idfl, ismooved=False, dt=dt):
                self.logger.log_file_error(idfl, FileNotFoundError("Файл не найден в старой структуре"))
                return False
            
            old_hash = self.file_ops.get_file_hash(idfl, ismooved=False, dt=dt, algorithm="md5")
            
            old_path = self.file_ops.base_path / idfl
            new_path = self.file_ops.move_file(idfl, dt)
            
            new_hash = self.file_ops.get_file_hash(idfl, ismooved=True, dt=dt, algorithm="md5")
            
            if old_hash and new_hash and old_hash != new_hash:
                self.logger.log_file_error(idfl, ValueError("Ошибка целостности файла после перемещения"))
                return False
            
            if not self.db.mark_file_moved(idfl, datetime.now()):
                self.logger.log_database_error("mark_file_moved", Exception(f"Не удалось отметить файл {idfl} как перемещенный"))
                return False
            
            self.logger.log_file_moved(idfl, old_path, new_path)
            return True
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            return False
    
    def migrate_all(self, max_files: Optional[int] = None) -> MigrationStats:
        """Мигрирует все файлы."""
        self.stats.start_time = datetime.now()
        
        try:
            if not self.initialize():
                raise MigrationError("Не удалось инициализировать мигратор")
            
            self.logger.log_migration_start(
                total_files=self.stats.total_files,
                batch_size=self.config.migrator.batch_size
            )
            
            while True:
                if max_files and self.stats.processed_files >= max_files:
                    self.logger.log_system_info(f"Достигнут лимит файлов: {max_files}")
                    break
                
                batch_size = self.config.migrator.batch_size
                if max_files:
                    remaining = max_files - self.stats.processed_files
                    batch_size = min(batch_size, remaining)
                
                processed, successful, failed = self.migrate_batch(batch_size)
                
                if processed == 0:
                    self.logger.log_system_info("Все файлы обработаны")
                    break
                
                progress = (self.stats.processed_files / self.stats.total_files) * 100 if self.stats.total_files > 0 else 0
                self.logger.log_progress(self.stats.processed_files, self.stats.total_files, progress)
                
                if self.config.migrator.retry_delay > 0:
                    time.sleep(self.config.migrator.retry_delay)
            
            self.stats.end_time = datetime.now()
            
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
        """Мигрирует файлы в указанном диапазоне дат."""
        self.stats.start_time = datetime.now()
        
        try:
            if not self.initialize():
                raise MigrationError("Не удалось инициализировать мигратор")
            
            files = self.db.get_files_by_date_range(start_date, end_date)
            unmoved_files = [f for f in files if not f['ismooved']]
            
            self.stats.total_files = len(unmoved_files)
            
            self.logger.log_system_info(f"Миграция файлов в диапазоне {start_date.date()} - {end_date.date()}")
            self.logger.log_system_info(f"Найдено файлов: {len(files)}, не перемещенных: {len(unmoved_files)}")
            
            if not unmoved_files:
                self.logger.log_system_info("Нет файлов для миграции в указанном диапазоне")
                self.stats.end_time = datetime.now()
                return self.stats
            
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
        """Проверяет корректность миграции на выборке файлов."""
        try:
            self.logger.log_system_info(f"Проверка миграции на выборке {sample_size} файлов")
            
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
                    if not self.file_ops.file_exists(idfl, ismooved=True, dt=dt):
                        errors += 1
                        details.append(f"Файл {idfl} не найден в новой структуре")
                        continue
                    
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
        """Получает текущий статус миграции."""
        try:
            db_stats = self.db.get_migration_statistics()
            fs_stats = self.file_ops.get_storage_statistics()
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
            
            removed_dirs = self.file_ops.cleanup_empty_directories()
            if removed_dirs > 0:
                self.logger.log_system_info(f"Удалено пустых каталогов: {removed_dirs}")
            
            self.db.close()
            self.logger.log_system_info("Очистка завершена")
            
        except Exception as e:
            self.logger.log_database_error("cleanup", e)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


def create_migrator(config: Config, logger: FileMigratorLogger) -> Migrator:
    """Удобная функция для создания объекта мигратора."""
    return Migrator(config, logger)


# ==================== CLI ИНТЕРФЕЙС ====================

class FileMigratorCLI:
    """Класс для обработки команд CLI."""
    
    def __init__(self):
        self.config = None
        self.logger = None
        self.migrator = None
    
    def setup(self, config_path: str = "config/settings.ini") -> bool:
        """Инициализирует CLI с конфигурацией."""
        try:
            self.config = load_config(config_path)
            self.logger = FileMigratorLogger(self.config.logging)
            self.migrator = create_migrator(self.config, self.logger)
            
            self.logger.log_system_info(f"Конфигурация загружена из: {config_path}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка инициализации: {e}")
            return False
    
    def cmd_migrate(self, args) -> int:
        """Команда миграции файлов."""
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            initial_status = self.migrator.get_migration_status()
            total_files = initial_status['database']['unmoved_files']
            
            if total_files == 0:
                print("✅ Нет файлов для миграции")
                return 0
            
            print(f"🚀 Начало миграции {total_files} файлов")
            
            if args.max_files:
                stats = self.migrator.migrate_all(max_files=args.max_files)
            else:
                stats = self.migrator.migrate_all()
            
            print(f"\n✅ Миграция завершена!")
            print(f"📊 Статистика:")
            print(f"   • Обработано: {stats.processed_files}")
            print(f"   • Успешно: {stats.successful_files}")
            print(f"   • Ошибок: {stats.failed_files}")
            print(f"   • Процент успеха: {stats.get_success_rate():.1f}%")
            print(f"   • Продолжительность: {stats.get_duration():.2f} сек")
            
            if stats.failed_files > 0:
                print(f"\n⚠️ Обнаружено {stats.failed_files} ошибок:")
                for error in stats.errors[:10]:
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
        """Команда миграции одного батча."""
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
    
    def cmd_status(self, args) -> int:
        """Команда просмотра статуса миграции."""
        try:
            if not self.migrator.initialize():
                print("❌ Не удалось инициализировать мигратор")
                return 1
            
            status = self.migrator.get_migration_status()
            
            print("📊 Статус миграции файлов")
            print("=" * 50)
            
            db_stats = status['database']
            print(f"\n🗄️ База данных:")
            print(f"   • Всего файлов: {db_stats.get('total_files', 0)}")
            print(f"   • Перемещено: {db_stats.get('moved_files', 0)}")
            print(f"   • Не перемещено: {db_stats.get('unmoved_files', 0)}")
            
            if db_stats.get('earliest_file_date'):
                print(f"   • Первый файл: {db_stats['earliest_file_date']}")
            if db_stats.get('latest_file_date'):
                print(f"   • Последний файл: {db_stats['latest_file_date']}")
            
            fs_stats = status['filesystem']
            print(f"\n📁 Файловая система:")
            print(f"   • Не перемещенных файлов: {fs_stats.get('unmoved_files_count', 0)}")
            print(f"   • Размер не перемещенных: {fs_stats.get('unmoved_files_size', 0):,} байт")
            print(f"   • Перемещенных файлов: {fs_stats.get('moved_files_count', 0)}")
            print(f"   • Размер перемещенных: {fs_stats.get('moved_files_size', 0):,} байт")
            print(f"   • Каталогов по датам: {fs_stats.get('date_directories_count', 0)}")
            
            total_files = db_stats.get('total_files', 0)
            moved_files = db_stats.get('moved_files', 0)
            if total_files > 0:
                progress = (moved_files / total_files) * 100
                print(f"\n📈 Прогресс миграции:")
                print(f"   • Завершено: {progress:.1f}%")
                print(f"   • Осталось: {total_files - moved_files} файлов")
            
            print(f"\n⏰ Время: {status['timestamp']}")
            
            return 0
            
        except Exception as e:
            print(f"❌ Ошибка получения статуса: {e}")
            return 1
        finally:
            if self.migrator:
                self.migrator.cleanup()
    
    def cmd_verify(self, args) -> int:
        """Команда проверки корректности миграции."""
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
    
    def cmd_test_connection(self, args) -> int:
        """Команда тестирования подключения к БД."""
        try:
            db = Database(self.config.database, self.logger)
            
            print("🔌 Тестирование подключения к базе данных...")
            
            if db.test_connection():
                print("✅ Подключение к БД успешно!")
                
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


def create_parser() -> argparse.ArgumentParser:
    """Создает парсер аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description="Монолитная утилита миграции файлов в структуру по датам",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Миграция всех файлов
  python migrator-mnlt.py migrate

  # Миграция с ограничением количества
  python migrator-mnlt.py migrate --max-files 1000

  # Миграция одного батча
  python migrator-mnlt.py migrate-batch --batch-size 50

  # Просмотр статуса
  python migrator-mnlt.py status

  # Проверка корректности миграции
  python migrator-mnlt.py verify --sample-size 200

  # Тестирование подключения к БД
  python migrator-mnlt.py test-connection
        """
    )
    
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
    
    # Команда test-connection
    subparsers.add_parser('test-connection', help='Тестирование подключения к БД')
    
    return parser


def main():
    """Главная функция CLI."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    cli = FileMigratorCLI()
    
    if not cli.setup(args.config):
        return 1
    
    try:
        if args.command == 'migrate':
            return cli.cmd_migrate(args)
        elif args.command == 'migrate-batch':
            return cli.cmd_migrate_batch(args)
        elif args.command == 'status':
            return cli.cmd_status(args)
        elif args.command == 'verify':
            return cli.cmd_verify(args)
        elif args.command == 'test-connection':
            return cli.cmd_test_connection(args)
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
