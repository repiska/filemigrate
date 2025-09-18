"""
Тесты для модуля config_loader.py
"""

import pytest
import tempfile
import os
from pathlib import Path
from src.config_loader import ConfigLoader, load_config, Config


class TestConfigLoader:
    """Тесты для класса ConfigLoader."""
    
    def test_load_config_success(self):
        """Тест успешной загрузки конфигурации."""
        config = load_config()
        
        # Проверяем основные секции
        assert config.database.driver == "mysql"
        assert config.database.host == "localhost"
        assert config.database.port == 3306
        assert config.database.database == "FileMigratorTest"
        assert config.database.username == "migrator_user"
        assert config.database.password == "migrator_pass123"
        
        assert config.paths.file_path == Path("test_files")
        assert config.paths.new_file_path == Path("test_files")
        
        assert config.migrator.batch_size == 1000
        assert config.migrator.max_retries == 3
        assert config.migrator.retry_delay == 1.0
        
        assert config.logging.level == "INFO"
        assert config.logging.log_file == Path("logs/migrator.log")
        assert config.logging.max_log_size == 10
        assert config.logging.backup_count == 5
    
    def test_config_file_not_found(self):
        """Тест ошибки при отсутствии файла конфигурации."""
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent_config.ini")
    
    def test_invalid_config_section(self):
        """Тест ошибки при отсутствии секции конфигурации."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("[invalid_section]\nkey=value\n")
            temp_config = f.name
        
        try:
            with pytest.raises(ValueError, match="Секция 'database' не найдена"):
                load_config(temp_config)
        finally:
            os.unlink(temp_config)
    
    def test_invalid_batch_size(self):
        """Тест валидации некорректного размера батча."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""[database]
driver = mysql
host = localhost
port = 3306
database = test
username = user
password = pass

[paths]
file_path = test_files
new_file_path = test_files

[migrator]
batch_size = -1
max_retries = 3
retry_delay = 1

[logging]
level = INFO
log_file = logs/test.log
max_log_size = 10
backup_count = 5
""")
            temp_config = f.name
        
        try:
            with pytest.raises(ValueError, match="Размер батча должен быть больше 0"):
                load_config(temp_config)
        finally:
            os.unlink(temp_config)
    
    def test_invalid_log_level(self):
        """Тест валидации некорректного уровня логирования."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""[database]
driver = mysql
host = localhost
port = 3306
database = test
username = user
password = pass

[paths]
file_path = test_files
new_file_path = test_files

[migrator]
batch_size = 1000
max_retries = 3
retry_delay = 1

[logging]
level = INVALID_LEVEL
log_file = logs/test.log
max_log_size = 10
backup_count = 5
""")
            temp_config = f.name
        
        try:
            with pytest.raises(ValueError, match="Некорректный уровень логирования"):
                load_config(temp_config)
        finally:
            os.unlink(temp_config)
    
    def test_pyodbc_config(self):
        """Тест загрузки конфигурации для SQL Server (pyodbc)."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""[database]
driver = pyodbc
server = localhost
port = 1433
database = test_db
username = user
password = pass
trusted_connection = true

[paths]
file_path = test_files
new_file_path = test_files

[migrator]
batch_size = 1000
max_retries = 3
retry_delay = 1

[logging]
level = INFO
log_file = logs/test.log
max_log_size = 10
backup_count = 5
""")
            temp_config = f.name
        
        try:
            config = load_config(temp_config)
            assert config.database.driver == "pyodbc"
            assert config.database.host == "localhost"
            assert config.database.port == 1433
            assert config.database.trusted_connection is True
        finally:
            os.unlink(temp_config)
    
    def test_reload_config(self):
        """Тест перезагрузки конфигурации."""
        loader = ConfigLoader()
        config1 = loader.load_config()
        config2 = loader.reload_config()
        
        # Конфигурации должны быть одинаковыми
        assert config1.database.database == config2.database.database
        assert config1.migrator.batch_size == config2.migrator.batch_size
    
    def test_get_config_without_load(self):
        """Тест получения конфигурации без предварительной загрузки."""
        loader = ConfigLoader()
        
        with pytest.raises(ValueError, match="Конфигурация не загружена"):
            loader.get_config()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
