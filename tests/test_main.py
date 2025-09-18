"""
Тесты для модуля main.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import argparse
from datetime import datetime

from src.main import FileMigratorCLI, create_parser


class TestFileMigratorCLI:
    """Тесты для класса FileMigratorCLI."""
    
    @pytest.fixture
    def mock_config(self):
        """Создает мок конфигурации."""
        from src.config_loader import Config, DatabaseConfig, PathsConfig, MigratorConfig, LoggingConfig
        
        return Config(
            database=DatabaseConfig(
                driver='mysql',
                host='localhost',
                port=3306,
                database='test_db',
                username='test_user',
                password='test_password'
            ),
            paths=PathsConfig(
                file_path='test_files',
                new_file_path='test_files'
            ),
            migrator=MigratorConfig(
                batch_size=100,
                max_retries=3,
                retry_delay=1.0
            ),
            logging=LoggingConfig(
                level='INFO',
                log_file='logs/test.log',
                max_log_size=10,
                backup_count=5
            )
        )
    
    @pytest.fixture
    def mock_logger(self):
        """Создает мок логгера."""
        return Mock()
    
    @pytest.fixture
    def mock_migrator(self):
        """Создает мок мигратора."""
        return Mock()
    
    @patch('src.main.load_config')
    @patch('src.main.FileMigratorLogger')
    @patch('src.main.create_migrator')
    def test_setup_success(self, mock_create_migrator, mock_logger_class, mock_load_config, mock_config):
        """Тест успешной инициализации CLI."""
        mock_load_config.return_value = mock_config
        mock_logger_instance = Mock()
        mock_logger_class.return_value = mock_logger_instance
        mock_migrator_instance = Mock()
        mock_create_migrator.return_value = mock_migrator_instance
        
        cli = FileMigratorCLI()
        result = cli.setup("test_config.ini")
        
        assert result is True
        assert cli.config == mock_config
        assert cli.logger == mock_logger_instance
        assert cli.migrator == mock_migrator_instance
        
        mock_load_config.assert_called_once_with("test_config.ini")
        mock_logger_class.assert_called_once_with(mock_config.logging)
        mock_create_migrator.assert_called_once_with(mock_config, mock_logger_instance)
    
    @patch('src.main.load_config')
    def test_setup_failure(self, mock_load_config):
        """Тест неудачной инициализации CLI."""
        mock_load_config.side_effect = Exception("Config error")
        
        cli = FileMigratorCLI()
        result = cli.setup("test_config.ini")
        
        assert result is False
        assert cli.config is None
        assert cli.logger is None
        assert cli.migrator is None
    
    @patch('src.main.Database')
    def test_cmd_test_connection_success(self, mock_db_class, mock_config, mock_logger):
        """Тест успешного тестирования подключения к БД."""
        mock_db_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_db_instance.test_connection.return_value = True
        mock_db_instance.get_migration_statistics.return_value = {
            'total_files': 100,
            'moved_files': 50,
            'unmoved_files': 50
        }
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        
        args = Mock()
        result = cli.cmd_test_connection(args)
        
        assert result == 0
        mock_db_instance.test_connection.assert_called_once()
        mock_db_instance.get_migration_statistics.assert_called_once()
        mock_db_instance.close.assert_called_once()
    
    @patch('src.main.Database')
    def test_cmd_test_connection_failure(self, mock_db_class, mock_config, mock_logger):
        """Тест неудачного тестирования подключения к БД."""
        mock_db_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_db_instance.test_connection.return_value = False
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        
        args = Mock()
        result = cli.cmd_test_connection(args)
        
        assert result == 1
        mock_db_instance.test_connection.assert_called_once()
        mock_db_instance.close.assert_called_once()
    
    def test_cmd_migrate_success(self, mock_config, mock_logger, mock_migrator):
        """Тест успешной миграции."""
        mock_migrator.initialize.return_value = True
        mock_migrator.get_migration_status.return_value = {
            'database': {'unmoved_files': 10}
        }
        
        # Мокаем статистику миграции
        mock_stats = Mock()
        mock_stats.processed_files = 10
        mock_stats.successful_files = 10
        mock_stats.failed_files = 0
        mock_stats.get_success_rate.return_value = 100.0
        mock_stats.get_duration.return_value = 5.0
        mock_stats.errors = []
        mock_migrator.migrate_all.return_value = mock_stats
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.max_files = None
        
        result = cli.cmd_migrate(args)
        
        assert result == 0
        mock_migrator.initialize.assert_called_once()
        mock_migrator.migrate_all.assert_called_once()
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_migrate_initialization_failure(self, mock_config, mock_logger, mock_migrator):
        """Тест неудачной инициализации мигратора."""
        mock_migrator.initialize.return_value = False
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        result = cli.cmd_migrate(args)
        
        assert result == 1
        mock_migrator.initialize.assert_called_once()
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_migrate_batch_success(self, mock_config, mock_logger, mock_migrator):
        """Тест успешной миграции батча."""
        mock_migrator.initialize.return_value = True
        mock_migrator.migrate_batch.return_value = (5, 5, 0)  # processed, successful, failed
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.batch_size = 50
        
        result = cli.cmd_migrate_batch(args)
        
        assert result == 0
        mock_migrator.initialize.assert_called_once()
        mock_migrator.migrate_batch.assert_called_once_with(50)
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_migrate_batch_no_files(self, mock_config, mock_logger, mock_migrator):
        """Тест миграции батча без файлов."""
        mock_migrator.initialize.return_value = True
        mock_migrator.migrate_batch.return_value = (0, 0, 0)  # processed, successful, failed
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.batch_size = None
        
        result = cli.cmd_migrate_batch(args)
        
        assert result == 0
        mock_migrator.migrate_batch.assert_called_once_with(100)  # batch_size из конфигурации
    
    def test_cmd_migrate_date_range_success(self, mock_config, mock_logger, mock_migrator):
        """Тест успешной миграции по диапазону дат."""
        mock_migrator.initialize.return_value = True
        
        # Мокаем статистику миграции
        mock_stats = Mock()
        mock_stats.processed_files = 3
        mock_stats.successful_files = 3
        mock_stats.failed_files = 0
        mock_stats.get_duration.return_value = 2.0
        mock_migrator.migrate_by_date_range.return_value = mock_stats
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.start_date = "2024-01-01"
        args.end_date = "2024-01-31"
        
        result = cli.cmd_migrate_date_range(args)
        
        assert result == 0
        mock_migrator.initialize.assert_called_once()
        mock_migrator.migrate_by_date_range.assert_called_once()
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_migrate_date_range_invalid_date(self, mock_config, mock_logger, mock_migrator):
        """Тест миграции по диапазону дат с неверным форматом даты."""
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.start_date = "invalid-date"
        args.end_date = "2024-01-31"
        
        result = cli.cmd_migrate_date_range(args)
        
        assert result == 1
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_status_success(self, mock_config, mock_logger, mock_migrator):
        """Тест успешного получения статуса."""
        mock_migrator.initialize.return_value = True
        mock_migrator.get_migration_status.return_value = {
            'database': {
                'total_files': 100,
                'moved_files': 50,
                'unmoved_files': 50,
                'earliest_file_date': '2024-01-01',
                'latest_file_date': '2024-01-31'
            },
            'filesystem': {
                'unmoved_files_count': 50,
                'unmoved_files_size': 1000000,
                'moved_files_count': 50,
                'moved_files_size': 1000000,
                'date_directories_count': 5
            },
            'timestamp': '2024-01-01T12:00:00'
        }
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        result = cli.cmd_status(args)
        
        assert result == 0
        mock_migrator.initialize.assert_called_once()
        mock_migrator.get_migration_status.assert_called_once()
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_verify_success(self, mock_config, mock_logger, mock_migrator):
        """Тест успешной проверки миграции."""
        mock_migrator.initialize.return_value = True
        mock_migrator.verify_migration.return_value = {
            'total_checked': 10,
            'verified': 10,
            'errors': 0,
            'details': []
        }
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.sample_size = 50
        
        result = cli.cmd_verify(args)
        
        assert result == 0
        mock_migrator.initialize.assert_called_once()
        mock_migrator.verify_migration.assert_called_once_with(50)
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_verify_with_errors(self, mock_config, mock_logger, mock_migrator):
        """Тест проверки миграции с ошибками."""
        mock_migrator.initialize.return_value = True
        mock_migrator.verify_migration.return_value = {
            'total_checked': 10,
            'verified': 8,
            'errors': 2,
            'details': ['File1 not found', 'File2 corrupted']
        }
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.sample_size = 10
        
        result = cli.cmd_verify(args)
        
        assert result == 1
        mock_migrator.verify_migration.assert_called_once_with(10)
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_cleanup_success(self, mock_config, mock_logger, mock_migrator):
        """Тест успешной очистки."""
        mock_migrator.initialize.return_value = True
        mock_migrator.file_ops = Mock()
        mock_migrator.file_ops.cleanup_empty_directories.return_value = 2
        mock_migrator.file_ops.get_storage_statistics.return_value = {
            'date_directories_count': 5
        }
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        result = cli.cmd_cleanup(args)
        
        assert result == 0
        mock_migrator.initialize.assert_called_once()
        mock_migrator.file_ops.cleanup_empty_directories.assert_called_once()
        mock_migrator.file_ops.get_storage_statistics.assert_called_once()
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_list_files_unmoved(self, mock_config, mock_logger, mock_migrator):
        """Тест просмотра списка не перемещенных файлов."""
        mock_migrator.initialize.return_value = True
        mock_migrator.file_ops = Mock()
        
        # Мокаем файлы
        from pathlib import Path
        mock_files = [
            Mock(spec=Path, name='file1.txt', stat=Mock(return_value=Mock(st_size=100))),
            Mock(spec=Path, name='file2.txt', stat=Mock(return_value=Mock(st_size=200)))
        ]
        mock_migrator.file_ops.list_unmoved_files.return_value = mock_files
        
        cli = FileMigratorCLI()
        cli.config = mock_config
        cli.logger = mock_logger
        cli.migrator = mock_migrator
        
        args = Mock()
        args.type = 'unmoved'
        args.limit = 10
        
        result = cli.cmd_list_files(args)
        
        assert result == 0
        mock_migrator.initialize.assert_called_once()
        mock_migrator.file_ops.list_unmoved_files.assert_called_once()
        mock_migrator.cleanup.assert_called_once()
    
    def test_cmd_list_files_moved(self, mock_config, mock_logger, mock_migrator):
        """Тест просмотра списка перемещенных файлов."""
        mock_migrator.initialize.return_value = True
        
        # Мокаем подключение к БД
        with patch('src.main.Database') as mock_db_class:
            mock_db_instance = Mock()
            mock_db_class.return_value = mock_db_instance
            mock_db_instance.get_files_to_move.return_value = [
                {'IDFL': 'file1', 'filename': 'test1.txt', 'dt': datetime(2024, 1, 1), 'ismooved': True},
                {'IDFL': 'file2', 'filename': 'test2.txt', 'dt': datetime(2024, 1, 2), 'ismooved': True}
            ]
            
            # Мокаем контекстный менеджер
            mock_db_instance.__enter__ = Mock(return_value=mock_db_instance)
            mock_db_instance.__exit__ = Mock(return_value=None)
            
            cli = FileMigratorCLI()
            cli.config = mock_config
            cli.logger = mock_logger
            cli.migrator = mock_migrator
            
            args = Mock()
            args.type = 'moved'
            args.limit = 10
            
            result = cli.cmd_list_files(args)
            
            assert result == 0
            mock_migrator.initialize.assert_called_once()
            mock_migrator.cleanup.assert_called_once()


class TestCreateParser:
    """Тесты для функции create_parser."""
    
    def test_create_parser(self):
        """Тест создания парсера аргументов."""
        parser = create_parser()
        
        assert isinstance(parser, argparse.ArgumentParser)
        assert parser.description == "Утилита миграции файлов в структуру по датам"
        
        # Проверяем наличие основных аргументов
        try:
            args = parser.parse_args(['--help'])
        except SystemExit:
            # SystemExit ожидается при --help
            pass
    
    def test_parser_subcommands(self):
        """Тест подкоманд парсера."""
        parser = create_parser()
        
        # Проверяем, что все подкоманды доступны
        subcommands = ['migrate', 'migrate-batch', 'migrate-date-range', 'status', 
                      'verify', 'cleanup', 'test-connection', 'list-files']
        
        for subcommand in subcommands:
            # Пытаемся распарсить команду с --help
            try:
                parser.parse_args([subcommand, '--help'])
            except SystemExit:
                # SystemExit ожидается при --help
                pass
    
    def test_parser_arguments(self):
        """Тест аргументов парсера."""
        parser = create_parser()
        
        # Тестируем основные аргументы
        args = parser.parse_args(['--config', 'test.ini', '--verbose', 'status'])
        assert args.config == 'test.ini'
        assert args.verbose is True
        assert args.command == 'status'
        
        # Тестируем аргументы команды migrate
        args = parser.parse_args(['migrate', '--max-files', '100'])
        assert args.command == 'migrate'
        assert args.max_files == 100
        
        # Тестируем аргументы команды migrate-batch
        args = parser.parse_args(['migrate-batch', '--batch-size', '50'])
        assert args.command == 'migrate-batch'
        assert args.batch_size == 50
        
        # Тестируем аргументы команды migrate-date-range
        args = parser.parse_args(['migrate-date-range', '--start-date', '2024-01-01', '--end-date', '2024-01-31'])
        assert args.command == 'migrate-date-range'
        assert args.start_date == '2024-01-01'
        assert args.end_date == '2024-01-31'
        
        # Тестируем аргументы команды verify
        args = parser.parse_args(['verify', '--sample-size', '200'])
        assert args.command == 'verify'
        assert args.sample_size == 200
        
        # Тестируем аргументы команды list-files
        args = parser.parse_args(['list-files', '--type', 'unmoved', '--limit', '15'])
        assert args.command == 'list-files'
        assert args.type == 'unmoved'
        assert args.limit == 15


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
