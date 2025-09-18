"""
Модуль для загрузки и валидации конфигурации приложения.

Обеспечивает централизованную загрузку параметров из config/settings.ini
с валидацией и удобным доступом к настройкам.
"""

import configparser
import os
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


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


class ConfigLoader:
    """Класс для загрузки и валидации конфигурации."""
    
    def __init__(self, config_path: str = "config/settings.ini"):
        """
        Инициализация загрузчика конфигурации.
        
        Args:
            config_path: Путь к файлу конфигурации
        """
        self.config_path = Path(config_path)
        self._config: Optional[Config] = None
    
    def load_config(self) -> Config:
        """
        Загружает конфигурацию из файла.
        
        Returns:
            Config: Объект конфигурации
            
        Raises:
            FileNotFoundError: Если файл конфигурации не найден
            ValueError: Если конфигурация некорректна
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Файл конфигурации не найден: {self.config_path}")
        
        config_parser = configparser.ConfigParser()
        config_parser.read(self.config_path, encoding='utf-8')
        
        try:
            # Загрузка конфигурации базы данных
            db_config = self._load_database_config(config_parser)
            
            # Загрузка конфигурации путей
            paths_config = self._load_paths_config(config_parser)
            
            # Загрузка конфигурации мигратора
            migrator_config = self._load_migrator_config(config_parser)
            
            # Загрузка конфигурации логирования
            logging_config = self._load_logging_config(config_parser)
            
            self._config = Config(
                database=db_config,
                paths=paths_config,
                migrator=migrator_config,
                logging=logging_config
            )
            
            # Валидация конфигурации
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
        elif driver == 'pyodbc':
            return DatabaseConfig(
                driver=driver,
                host=parser.get(section, 'server'),
                port=parser.getint(section, 'port', fallback=1433),
                database=parser.get(section, 'database'),
                username=parser.get(section, 'username'),
                password=parser.get(section, 'password'),
                trusted_connection=parser.getboolean(section, 'trusted_connection', fallback=False)
            )
        else:
            raise ValueError(f"Неподдерживаемый драйвер БД: {driver}")
    
    def _load_paths_config(self, parser: configparser.ConfigParser) -> PathsConfig:
        """Загружает конфигурацию путей."""
        section = 'paths'
        
        if not parser.has_section(section):
            raise ValueError(f"Секция '{section}' не найдена в конфигурации")
        
        file_path = Path(parser.get(section, 'file_path'))
        new_file_path = Path(parser.get(section, 'new_file_path'))
        
        return PathsConfig(
            file_path=file_path,
            new_file_path=new_file_path
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
        
        log_file = Path(parser.get(section, 'log_file', fallback='logs/migrator.log'))
        
        return LoggingConfig(
            level=parser.get(section, 'level', fallback='INFO'),
            log_file=log_file,
            max_log_size=parser.getint(section, 'max_log_size', fallback=10),
            backup_count=parser.getint(section, 'backup_count', fallback=5)
        )
    
    def _validate_config(self) -> None:
        """Валидирует загруженную конфигурацию."""
        if not self._config:
            raise ValueError("Конфигурация не загружена")
        
        # Проверка путей
        if not self._config.paths.file_path.exists():
            raise ValueError(f"Путь к файлам не существует: {self._config.paths.file_path}")
        
        # Проверка параметров мигратора
        if self._config.migrator.batch_size <= 0:
            raise ValueError("Размер батча должен быть больше 0")
        
        if self._config.migrator.max_retries < 0:
            raise ValueError("Количество попыток не может быть отрицательным")
        
        if self._config.migrator.retry_delay < 0:
            raise ValueError("Задержка между попытками не может быть отрицательной")
        
        # Проверка уровня логирования
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self._config.logging.level.upper() not in valid_levels:
            raise ValueError(f"Некорректный уровень логирования: {self._config.logging.level}")
    
    def get_config(self) -> Config:
        """
        Возвращает загруженную конфигурацию.
        
        Returns:
            Config: Объект конфигурации
            
        Raises:
            ValueError: Если конфигурация не загружена
        """
        if self._config is None:
            raise ValueError("Конфигурация не загружена. Вызовите load_config() сначала.")
        return self._config
    
    def reload_config(self) -> Config:
        """
        Перезагружает конфигурацию из файла.
        
        Returns:
            Config: Обновленный объект конфигурации
        """
        self._config = None
        return self.load_config()


def load_config(config_path: str = "config/settings.ini") -> Config:
    """
    Удобная функция для быстрой загрузки конфигурации.
    
    Args:
        config_path: Путь к файлу конфигурации
        
    Returns:
        Config: Объект конфигурации
    """
    loader = ConfigLoader(config_path)
    return loader.load_config()


if __name__ == "__main__":
    # Тестирование модуля
    try:
        config = load_config()
        print("✅ Конфигурация успешно загружена!")
        print(f"📁 Путь к файлам: {config.paths.file_path}")
        print(f"🗄️ База данных: {config.database.database}")
        print(f"📊 Размер батча: {config.migrator.batch_size}")
        print(f"📝 Уровень логирования: {config.logging.level}")
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
