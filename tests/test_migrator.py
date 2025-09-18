"""
Тесты для модуля migrator.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import Dict, List

from src.migrator import Migrator, MigrationStats, MigrationError, create_migrator
from src.config_loader import Config, MigratorConfig, DatabaseConfig, PathsConfig, LoggingConfig
from src.logger import FileMigratorLogger


class TestMigrationStats:
    """Тесты для класса MigrationStats."""
    
    def test_migration_stats_initialization(self):
        """Тест инициализации статистики миграции."""
        stats = MigrationStats()
        
        assert stats.total_files == 0
        assert stats.processed_files == 0
        assert stats.successful_files == 0
        assert stats.failed_files == 0
        assert stats.skipped_files == 0
        assert stats.start_time is None
        assert stats.end_time is None
        assert stats.batch_count == 0
        assert stats.errors == []
    
    def test_add_error(self):
        """Тест добавления ошибки."""
        stats = MigrationStats()
        error = Exception("Test error")
        
        stats.add_error("file001", error)
        
        assert len(stats.errors) == 1
        assert stats.errors[0]['file_id'] == "file001"
        assert stats.errors[0]['error'] == "Test error"
        assert 'timestamp' in stats.errors[0]
    
    def test_get_duration(self):
        """Тест получения продолжительности миграции."""
        stats = MigrationStats()
        
        # Без времени начала и окончания
        assert stats.get_duration() is None
        
        # С временем начала и окончания
        stats.start_time = datetime(2024, 1, 1, 10, 0, 0)
        stats.end_time = datetime(2024, 1, 1, 10, 5, 0)
        
        duration = stats.get_duration()
        assert duration == 300.0  # 5 минут
    
    def test_get_success_rate(self):
        """Тест получения процента успешных миграций."""
        stats = MigrationStats()
        
        # Без обработанных файлов
        assert stats.get_success_rate() == 0.0
        
        # С обработанными файлами
        stats.processed_files = 100
        stats.successful_files = 80
        
        assert stats.get_success_rate() == 80.0
    
    def test_to_dict(self):
        """Тест преобразования в словарь."""
        stats = MigrationStats()
        stats.total_files = 100
        stats.processed_files = 50
        stats.successful_files = 45
        stats.failed_files = 5
        stats.batch_count = 2
        stats.start_time = datetime(2024, 1, 1, 10, 0, 0)
        stats.end_time = datetime(2024, 1, 1, 10, 5, 0)
        
        result = stats.to_dict()
        
        assert result['total_files'] == 100
        assert result['processed_files'] == 50
        assert result['successful_files'] == 45
        assert result['failed_files'] == 5
        assert result['batch_count'] == 2
        assert result['start_time'] == '2024-01-01T10:00:00'
        assert result['end_time'] == '2024-01-01T10:05:00'
        assert result['duration_seconds'] == 300.0
        assert result['success_rate'] == 90.0
        assert result['error_count'] == 0


class TestMigrator:
    """Тесты для класса Migrator."""
    
    @pytest.fixture
    def mock_config(self):
        """Создает мок конфигурации."""
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
        return Mock(spec=FileMigratorLogger)
    
    @pytest.fixture
    def mock_db(self):
        """Создает мок базы данных."""
        return Mock()
    
    @pytest.fixture
    def mock_file_ops(self):
        """Создает мок операций с файлами."""
        return Mock()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrator_initialization(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест инициализации мигратора."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        migrator = Migrator(mock_config, mock_logger)
        
        assert migrator.config == mock_config
        assert migrator.logger == mock_logger
        assert migrator.db == mock_db_instance
        assert migrator.file_ops == mock_file_ops_instance
        assert isinstance(migrator.stats, MigrationStats)
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_initialize_success(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест успешной инициализации."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        mock_db_instance.test_connection.return_value = True
        mock_db_instance.get_migration_statistics.return_value = {
            'total_files': 100,
            'unmoved_files': 50
        }
        mock_file_ops_instance.get_storage_statistics.return_value = {
            'unmoved_files_count': 50
        }
        
        migrator = Migrator(mock_config, mock_logger)
        result = migrator.initialize()
        
        assert result is True
        assert migrator.stats.total_files == 50
        mock_logger.log_system_info.assert_called()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_initialize_db_connection_failed(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест неудачной инициализации из-за БД."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        mock_db_instance.test_connection.return_value = False
        
        migrator = Migrator(mock_config, mock_logger)
        result = migrator.initialize()
        
        assert result is False
        mock_logger.log_critical_error.assert_called()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrate_batch_success(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест успешной миграции батча."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        test_files = [
            {'IDFL': 'file001', 'dt': datetime(2024, 1, 15), 'filename': 'test1.txt', 'ismooved': False},
            {'IDFL': 'file002', 'dt': datetime(2024, 1, 16), 'filename': 'test2.txt', 'ismooved': False}
        ]
        mock_db_instance.get_files_to_move.return_value = test_files
        
        migrator = Migrator(mock_config, mock_logger)
        
        # Мокаем методы миграции
        with patch.object(migrator, '_migrate_single_file', return_value=True) as mock_migrate:
            processed, successful, failed = migrator.migrate_batch(2)
            
            assert processed == 2
            assert successful == 2
            assert failed == 0
            assert migrator.stats.batch_count == 1
            assert migrator.stats.processed_files == 2
            assert migrator.stats.successful_files == 2
            
            mock_logger.log_batch_start.assert_called_once_with(1, 2)
            mock_logger.log_batch_end.assert_called_once_with(1, 2, 2, 0)
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrate_batch_no_files(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест миграции батча без файлов."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        mock_db_instance.get_files_to_move.return_value = []
        
        migrator = Migrator(mock_config, mock_logger)
        processed, successful, failed = migrator.migrate_batch(10)
        
        assert processed == 0
        assert successful == 0
        assert failed == 0
        mock_logger.log_system_info.assert_called_with("Нет файлов для миграции")
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrate_single_file_success(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест успешной миграции одного файла."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        mock_file_ops_instance.file_exists.return_value = True
        mock_file_ops_instance.get_file_hash.return_value = "test_hash"  # Возвращаем валидный хеш
        mock_file_ops_instance.move_file.return_value = "new_path"
        from pathlib import Path
        mock_file_ops_instance.base_path = Path("test_path")  # Мокаем base_path как Path объект
        mock_db_instance.mark_file_moved.return_value = True  # mark_file_moved возвращает True при успехе
        
        migrator = Migrator(mock_config, mock_logger)
        
        file_info = {
            'IDFL': 'file001',
            'dt': datetime(2024, 1, 15),
            'filename': 'test.txt',
            'ismooved': False
        }
        
        result = migrator._migrate_single_file(file_info)
        
        assert result is True
        mock_file_ops_instance.file_exists.assert_called_once_with('file001', ismooved=False, dt=file_info['dt'])
        mock_file_ops_instance.move_file.assert_called_once_with('file001', file_info['dt'])
        mock_db_instance.mark_file_moved.assert_called_once()
        mock_logger.log_file_moved.assert_called_once()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrate_single_file_not_found(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест миграции несуществующего файла."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        mock_file_ops_instance.file_exists.return_value = False
        
        migrator = Migrator(mock_config, mock_logger)
        
        file_info = {
            'IDFL': 'file001',
            'dt': datetime(2024, 1, 15),
            'filename': 'test.txt',
            'ismooved': False
        }
        
        result = migrator._migrate_single_file(file_info)
        
        assert result is False
        mock_logger.log_file_error.assert_called_once()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrate_single_file_integrity_error(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест миграции файла с ошибкой целостности."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        mock_file_ops_instance.file_exists.return_value = True
        mock_file_ops_instance.get_file_hash.side_effect = ["old_hash", "new_hash"]  # Разные хеши
        mock_file_ops_instance.move_file.return_value = "new_path"
        
        migrator = Migrator(mock_config, mock_logger)
        
        file_info = {
            'IDFL': 'file001',
            'dt': datetime(2024, 1, 15),
            'filename': 'test.txt',
            'ismooved': False
        }
        
        result = migrator._migrate_single_file(file_info)
        
        assert result is False
        mock_logger.log_file_error.assert_called_once()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrate_all_success(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест успешной миграции всех файлов."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки для инициализации
        mock_db_instance.test_connection.return_value = True
        mock_db_instance.get_migration_statistics.return_value = {
            'total_files': 10,
            'unmoved_files': 5
        }
        mock_file_ops_instance.get_storage_statistics.return_value = {
            'unmoved_files_count': 5
        }
        
        # Настраиваем моки для миграции
        test_files = [
            {'IDFL': 'file001', 'dt': datetime(2024, 1, 15), 'filename': 'test1.txt', 'ismooved': False}
        ]
        mock_db_instance.get_files_to_move.return_value = test_files
        
        migrator = Migrator(mock_config, mock_logger)
        
        # Мокаем методы миграции
        with patch.object(migrator, '_migrate_single_file', return_value=True):
            with patch('time.sleep'):  # Мокаем sleep
                stats = migrator.migrate_all(max_files=1)
                
                assert stats.processed_files == 1
                assert stats.successful_files == 1
                assert stats.failed_files == 0
                assert stats.start_time is not None
                assert stats.end_time is not None
                
                mock_logger.log_migration_start.assert_called_once()
                mock_logger.log_migration_end.assert_called_once()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_migrate_by_date_range(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест миграции по диапазону дат."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки для инициализации
        mock_db_instance.test_connection.return_value = True
        mock_db_instance.get_migration_statistics.return_value = {
            'total_files': 10,
            'unmoved_files': 5
        }
        mock_file_ops_instance.get_storage_statistics.return_value = {
            'unmoved_files_count': 5
        }
        
        # Настраиваем моки для миграции по датам
        test_files = [
            {'IDFL': 'file001', 'dt': datetime(2024, 1, 15), 'filename': 'test1.txt', 'ismooved': False}
        ]
        mock_db_instance.get_files_by_date_range.return_value = test_files
        
        migrator = Migrator(mock_config, mock_logger)
        
        # Мокаем методы миграции
        with patch.object(migrator, '_migrate_single_file', return_value=True):
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 1, 31)
            stats = migrator.migrate_by_date_range(start_date, end_date)
            
            assert stats.processed_files == 1
            assert stats.successful_files == 1
            assert stats.failed_files == 0
            
            mock_db_instance.get_files_by_date_range.assert_called_once_with(start_date, end_date)
            mock_logger.log_migration_end.assert_called_once()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_verify_migration(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест проверки миграции."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        test_files = [
            {'IDFL': 'file001', 'dt': datetime(2024, 1, 15), 'filename': 'test1.txt', 'ismooved': True}
        ]
        mock_db_instance.get_files_to_move.return_value = test_files
        mock_file_ops_instance.file_exists.return_value = True
        mock_file_ops_instance.get_file_size.return_value = 100
        
        migrator = Migrator(mock_config, mock_logger)
        
        result = migrator.verify_migration(sample_size=1)
        
        assert result['verified'] == 1
        assert result['errors'] == 0
        assert result['total_checked'] == 1
        assert len(result['details']) == 0
        
        mock_logger.log_system_info.assert_called()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_get_migration_status(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест получения статуса миграции."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        db_stats = {'total_files': 100, 'moved_files': 50}
        fs_stats = {'unmoved_files_count': 50}
        mock_db_instance.get_migration_statistics.return_value = db_stats
        mock_file_ops_instance.get_storage_statistics.return_value = fs_stats
        
        migrator = Migrator(mock_config, mock_logger)
        
        status = migrator.get_migration_status()
        
        assert 'database' in status
        assert 'filesystem' in status
        assert 'migrator' in status
        assert 'timestamp' in status
        assert status['database'] == db_stats
        assert status['filesystem'] == fs_stats
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_cleanup(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест очистки ресурсов."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        # Настраиваем моки
        mock_file_ops_instance.cleanup_empty_directories.return_value = 2
        
        migrator = Migrator(mock_config, mock_logger)
        migrator.cleanup()
        
        mock_file_ops_instance.cleanup_empty_directories.assert_called_once()
        mock_db_instance.close.assert_called_once()
        mock_logger.log_system_info.assert_called()
    
    @patch('src.migrator.Database')
    @patch('src.migrator.FileOps')
    def test_context_manager(self, mock_file_ops_class, mock_db_class, mock_config, mock_logger):
        """Тест контекстного менеджера."""
        mock_db_instance = Mock()
        mock_file_ops_instance = Mock()
        mock_db_class.return_value = mock_db_instance
        mock_file_ops_class.return_value = mock_file_ops_instance
        
        with patch.object(Migrator, 'cleanup') as mock_cleanup:
            with Migrator(mock_config, mock_logger) as migrator:
                assert migrator is not None
            
            mock_cleanup.assert_called_once()


class TestCreateMigrator:
    """Тесты для функции create_migrator."""
    
    def test_create_migrator(self):
        """Тест создания объекта мигратора."""
        mock_config = Mock(spec=Config)
        mock_logger = Mock(spec=FileMigratorLogger)
        
        with patch('src.migrator.Migrator') as mock_migrator_class:
            mock_migrator_instance = Mock()
            mock_migrator_class.return_value = mock_migrator_instance
            
            result = create_migrator(mock_config, mock_logger)
            
            assert result == mock_migrator_instance
            mock_migrator_class.assert_called_once_with(mock_config, mock_logger)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
