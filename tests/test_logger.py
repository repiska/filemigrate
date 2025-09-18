"""
Тесты для модуля logger.py
"""

import pytest
import tempfile
import os
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.logger import (
    FileMigratorLogger, 
    setup_logger, 
    get_logger,
    ColoredFormatter
)
from src.config_loader import LoggingConfig


class TestColoredFormatter:
    """Тесты для ColoredFormatter."""
    
    def test_colored_formatter(self):
        """Тест цветного форматтера."""
        formatter = ColoredFormatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Создаем тестовую запись лога
        record = logging.LogRecord(
            name='test',
            level=logging.INFO,
            pathname='',
            lineno=0,
            msg='Test message',
            args=(),
            exc_info=None
        )
        
        formatted = formatter.format(record)
        
        # Проверяем, что формат содержит цветовые коды для уровня логирования
        assert '\033[32m' in formatted or '\x1b[32m' in formatted  # Зеленый цвет для INFO
        assert '\033[0m' in formatted or '\x1b[0m' in formatted   # Сброс цвета
        assert 'Test message' in formatted
        assert 'INFO' in formatted  # Уровень логирования должен быть в выводе


class TestFileMigratorLogger:
    """Тесты для FileMigratorLogger."""
    
    @pytest.fixture
    def temp_log_config(self):
        """Создает временную конфигурацию логирования."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False) as f:
            temp_log_file = f.name
        
        config = LoggingConfig(
            level='DEBUG',
            log_file=Path(temp_log_file),
            max_log_size=1,  # 1 MB
            backup_count=3
        )
        
        yield config
        
        # Очистка - закрываем все обработчики логгера
        logger = logging.getLogger('file_migrator')
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        
        # Удаляем временный файл
        try:
            os.unlink(temp_log_file)
        except (FileNotFoundError, PermissionError):
            pass
    
    def test_logger_initialization(self, temp_log_config):
        """Тест инициализации логгера."""
        logger = FileMigratorLogger(temp_log_config)
        assert logger.logger is not None
        assert logger.logger.name == 'file_migrator'
        assert logger.logger.level == logging.DEBUG
    
    def test_logger_handlers(self, temp_log_config):
        """Тест обработчиков логгера."""
        logger = FileMigratorLogger(temp_log_config)
        handlers = logger.logger.handlers
        
        # Должно быть 2 обработчика: файловый и консольный
        assert len(handlers) == 2
        
        # Проверяем типы обработчиков
        handler_types = [type(h).__name__ for h in handlers]
        assert 'RotatingFileHandler' in handler_types
        assert 'StreamHandler' in handler_types
    
    def test_log_migration_start(self, temp_log_config):
        """Тест логирования начала миграции."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_migration_start(1000, 100)
            
            # Проверяем, что вызвано 4 раза (4 сообщения)
            assert mock_info.call_count == 4
            
            # Проверяем содержимое сообщений
            calls = [call[0][0] for call in mock_info.call_args_list]
            assert any("🚀 Начало миграции файлов" in call for call in calls)
            assert any("📊 Всего файлов: 1000" in call for call in calls)
            assert any("📦 Размер батча: 100" in call for call in calls)
    
    def test_log_migration_end(self, temp_log_config):
        """Тест логирования завершения миграции."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_migration_end(100, 95, 5)
            
            # Проверяем количество вызовов
            assert mock_info.call_count == 6  # 6 сообщений
            
            # Проверяем содержимое
            calls = [call[0][0] for call in mock_info.call_args_list]
            assert any("✅ Миграция завершена" in call for call in calls)
            assert any("📊 Статистика:" in call for call in calls)
            assert any("• Обработано: 100" in call for call in calls)
            assert any("• Успешно: 95" in call for call in calls)
            assert any("• Ошибок: 5" in call for call in calls)
    
    def test_log_file_moved(self, temp_log_config):
        """Тест логирования перемещения файла."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            source_path = Path("old/path/file.txt")
            target_path = Path("new/path/file.txt")
            logger.log_file_moved("file001", source_path, target_path)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "📁 Файл file001 перемещен:" in call_args
            assert "old/path/file.txt" in call_args or "old\\path\\file.txt" in call_args
            assert "new/path/file.txt" in call_args or "new\\path\\file.txt" in call_args
    
    def test_log_file_error(self, temp_log_config):
        """Тест логирования ошибки файла."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'error') as mock_error:
            error = FileNotFoundError("File not found")
            logger.log_file_error("file001", error)
            
            mock_error.assert_called_once()
            call_args = mock_error.call_args[0][0]
            assert "❌ Ошибка при обработке файла file001:" in call_args
            assert "File not found" in call_args
    
    def test_log_database_error(self, temp_log_config):
        """Тест логирования ошибки БД."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'error') as mock_error:
            error = ConnectionError("Connection failed")
            logger.log_database_error("SELECT", error)
            
            mock_error.assert_called_once()
            call_args = mock_error.call_args[0][0]
            assert "🗄️ Ошибка БД при SELECT:" in call_args
            assert "Connection failed" in call_args
    
    def test_log_progress(self, temp_log_config):
        """Тест логирования прогресса."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_progress(50, 100)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "📈 Прогресс: 50/100 (50.0%)" in call_args
    
    def test_log_progress_with_percentage(self, temp_log_config):
        """Тест логирования прогресса с заданным процентом."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_progress(30, 100, 30.0)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "📈 Прогресс: 30/100 (30.0%)" in call_args
    
    def test_log_batch_start(self, temp_log_config):
        """Тест логирования начала батча."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_batch_start(1, 50)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "📦 Обработка батча #1 (размер: 50)" in call_args
    
    def test_log_batch_end(self, temp_log_config):
        """Тест логирования завершения батча."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_batch_end(1, 50, 48, 2)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "✅ Батч #1 завершен: 48/50 успешно, 2 ошибок" in call_args
    
    def test_log_config_loaded(self, temp_log_config):
        """Тест логирования загрузки конфигурации."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_config_loaded("config/settings.ini")
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "⚙️ Конфигурация загружена из config/settings.ini" in call_args
    
    def test_log_database_connected(self, temp_log_config):
        """Тест логирования подключения к БД."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_database_connected("test_db", "localhost", 3306)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "🗄️ Подключение к БД установлено: test_db@localhost:3306" in call_args
    
    def test_log_database_disconnected(self, temp_log_config):
        """Тест логирования отключения от БД."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_database_disconnected()
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "🗄️ Подключение к БД закрыто" in call_args
    
    def test_log_file_operation(self, temp_log_config):
        """Тест логирования операций с файлами."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            file_path = Path("test/file.txt")
            logger.log_file_operation("read", file_path, True)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "✅ READ: test/file.txt" in call_args or "✅ READ: test\\file.txt" in call_args
    
    def test_log_file_operation_failed(self, temp_log_config):
        """Тест логирования неудачной операции с файлом."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            file_path = Path("test/file.txt")
            logger.log_file_operation("write", file_path, False)
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "❌ WRITE: test/file.txt" in call_args or "❌ WRITE: test\\file.txt" in call_args
    
    def test_log_system_info(self, temp_log_config):
        """Тест логирования системной информации."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_system_info("System started")
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "ℹ️ System started" in call_args
    
    def test_log_warning(self, temp_log_config):
        """Тест логирования предупреждения."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'warning') as mock_warning:
            logger.log_warning("Low disk space")
            
            mock_warning.assert_called_once()
            call_args = mock_warning.call_args[0][0]
            assert "⚠️ Low disk space" in call_args
    
    def test_log_critical_error(self, temp_log_config):
        """Тест логирования критической ошибки."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'critical') as mock_critical:
            error = RuntimeError("System failure")
            logger.log_critical_error("Critical system error", error)
            
            mock_critical.assert_called_once()
            call_args = mock_critical.call_args[0][0]
            assert "💥 Critical system error:" in call_args
            assert "System failure" in call_args
    
    def test_log_critical_error_without_exception(self, temp_log_config):
        """Тест логирования критической ошибки без исключения."""
        logger = FileMigratorLogger(temp_log_config)
        
        with patch.object(logger.logger, 'critical') as mock_critical:
            logger.log_critical_error("Critical system error")
            
            mock_critical.assert_called_once()
            call_args = mock_critical.call_args[0][0]
            assert "💥 Critical system error" in call_args
    
    def test_get_logger(self, temp_log_config):
        """Тест получения логгера."""
        logger = FileMigratorLogger(temp_log_config)
        returned_logger = logger.get_logger()
        
        assert returned_logger is logger.logger
        assert returned_logger.name == 'file_migrator'


class TestSetupLogger:
    """Тесты для функции setup_logger."""
    
    def test_setup_logger(self):
        """Тест настройки логгера."""
        config = LoggingConfig(
            level='INFO',
            log_file=Path('test.log'),
            max_log_size=1,
            backup_count=3
        )
        
        logger = setup_logger(config)
        
        assert logger is not None
        assert logger.name == 'file_migrator'
        assert logger.level == logging.INFO


class TestGetLogger:
    """Тесты для функции get_logger."""
    
    def test_get_logger_default_name(self):
        """Тест получения логгера с именем по умолчанию."""
        logger = get_logger()
        assert logger.name == 'file_migrator'
    
    def test_get_logger_custom_name(self):
        """Тест получения логгера с пользовательским именем."""
        logger = get_logger('custom_logger')
        assert logger.name == 'custom_logger'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
