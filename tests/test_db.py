"""
Тесты для модуля db.py
"""

import pytest
import mysql.connector
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import List, Dict

from src.db import Database, DatabaseConnectionError, DatabaseQueryError, create_database
from src.config_loader import DatabaseConfig
from src.logger import FileMigratorLogger


class TestDatabase:
    """Тесты для класса Database."""
    
    @pytest.fixture
    def mock_config(self):
        """Создает мок конфигурации БД."""
        return DatabaseConfig(
            driver='mysql',
            host='localhost',
            port=3306,
            database='test_db',
            username='test_user',
            password='test_password'
        )
    
    @pytest.fixture
    def mock_logger(self):
        """Создает мок логгера."""
        logger = Mock(spec=FileMigratorLogger)
        return logger
    
    @pytest.fixture
    def mock_connection_pool(self):
        """Создает мок пула соединений."""
        pool = Mock()
        connection = Mock()
        connection.cursor.return_value.__enter__.return_value = Mock()
        connection.cursor.return_value.__exit__.return_value = None
        pool.get_connection.return_value = connection
        return pool
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_database_initialization(self, mock_pool_class, mock_config, mock_logger):
        """Тест инициализации базы данных."""
        mock_pool = Mock()
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        
        assert db.config == mock_config
        assert db.logger == mock_logger
        assert db.connection_pool == mock_pool
        mock_logger.log_database_connected.assert_called_once_with('test_db', 'localhost', 3306)
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_database_initialization_error(self, mock_pool_class, mock_config, mock_logger):
        """Тест ошибки инициализации базы данных."""
        mock_pool_class.side_effect = mysql.connector.Error("Connection failed")
        
        with pytest.raises(DatabaseConnectionError):
            Database(mock_config, mock_logger)
        
        mock_logger.log_database_error.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_connection(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения соединения из пула."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        connection = db._get_connection()
        
        assert connection == mock_connection
        mock_pool.get_connection.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_connection_error(self, mock_pool_class, mock_config, mock_logger):
        """Тест ошибки получения соединения."""
        mock_pool = Mock()
        mock_pool.get_connection.side_effect = mysql.connector.Error("Connection failed")
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        
        with pytest.raises(DatabaseConnectionError):
            db._get_connection()
        
        mock_logger.log_database_error.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_execute_query_success(self, mock_pool_class, mock_config, mock_logger):
        """Тест успешного выполнения запроса."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'test'}]
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db._execute_query("SELECT * FROM test", fetch=True)
        
        assert result == [{'id': 1, 'name': 'test'}]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test")
        mock_cursor.fetchall.assert_called_once()
        # Для SELECT запросов commit не вызывается
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_execute_query_with_params(self, mock_pool_class, mock_config, mock_logger):
        """Тест выполнения запроса с параметрами."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        db._execute_query("SELECT * FROM test WHERE id = %s", (1,))
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test WHERE id = %s", (1,))
        mock_connection.commit.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_execute_query_error(self, mock_pool_class, mock_config, mock_logger):
        """Тест ошибки выполнения запроса."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_cursor.execute.side_effect = mysql.connector.Error("Query failed")
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        
        with pytest.raises(DatabaseQueryError):
            db._execute_query("SELECT * FROM test")
        
        mock_connection.rollback.assert_called_once()
        mock_logger.log_database_error.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_test_connection_success(self, mock_pool_class, mock_config, mock_logger):
        """Тест успешного тестирования подключения."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_cursor.fetchone.return_value = (1,)
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.test_connection()
        
        assert result is True
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_logger.log_system_info.assert_called_once_with("Тест подключения к БД успешен")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_test_connection_failure(self, mock_pool_class, mock_config, mock_logger):
        """Тест неудачного тестирования подключения."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_cursor.execute.side_effect = mysql.connector.Error("Connection failed")
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.test_connection()
        
        assert result is False
        mock_logger.log_database_error.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_files_to_move(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения файлов для миграции."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        expected_files = [
            {'IDFL': 'file001', 'dt': datetime(2024, 1, 15), 'filename': 'test1.txt', 'ismooved': 0},
            {'IDFL': 'file002', 'dt': datetime(2024, 1, 16), 'filename': 'test2.txt', 'ismooved': 0}
        ]
        mock_cursor.fetchall.return_value = expected_files
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.get_files_to_move(10)
        
        assert result == expected_files
        mock_cursor.execute.assert_called_once()
        mock_logger.log_system_info.assert_called_once_with("Получено 2 файлов для миграции")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_total_files_count(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения общего количества файлов."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_cursor.fetchall.return_value = [{'total': 100}]
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.get_total_files_count()
        
        assert result == 100
        mock_logger.log_system_info.assert_called_once_with("Общее количество файлов в БД: 100")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_unmoved_files_count(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения количества не перемещенных файлов."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_cursor.fetchall.return_value = [{'total': 50}]
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.get_unmoved_files_count()
        
        assert result == 50
        mock_logger.log_system_info.assert_called_once_with("Количество не перемещенных файлов: 50")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_mark_file_moved(self, mock_pool_class, mock_config, mock_logger):
        """Тест отметки файла как перемещенного."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.mark_file_moved("file001")
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_connection.commit.assert_called_once()
        mock_logger.log_system_info.assert_called_once_with("Файл file001 отмечен как перемещенный")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_mark_file_moved_with_datetime(self, mock_pool_class, mock_config, mock_logger):
        """Тест отметки файла как перемещенного с указанной датой."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        test_datetime = datetime(2024, 1, 15, 10, 30)
        result = db.mark_file_moved("file001", test_datetime)
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        # Проверяем, что передана правильная дата
        call_args = mock_cursor.execute.call_args[0]
        assert call_args[1][0] == test_datetime
        assert call_args[1][1] == "file001"
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_insert_new_file(self, mock_pool_class, mock_config, mock_logger):
        """Тест добавления нового файла."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        test_datetime = datetime(2024, 1, 15, 10, 30)
        result = db.insert_new_file("file001", "test.txt", test_datetime)
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        mock_connection.commit.assert_called_once()
        mock_logger.log_system_info.assert_called_once_with("Добавлен новый файл: file001 (test.txt)")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_file_metadata(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения метаданных файла."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        expected_metadata = {
            'IDFL': 'file001',
            'dt': datetime(2024, 1, 15),
            'filename': 'test.txt',
            'ismooved': 0,
            'dtmoove': None
        }
        mock_cursor.fetchall.return_value = [expected_metadata]
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.get_file_metadata("file001")
        
        assert result == expected_metadata
        mock_cursor.execute.assert_called_once()
        mock_logger.log_system_info.assert_called_once_with("Получены метаданные для файла file001")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_file_metadata_not_found(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения метаданных несуществующего файла."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.get_file_metadata("nonexistent")
        
        assert result is None
        mock_logger.log_warning.assert_called_once_with("Файл nonexistent не найден в БД")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_files_by_date_range(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения файлов по диапазону дат."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        expected_files = [
            {'IDFL': 'file001', 'dt': datetime(2024, 1, 15), 'filename': 'test1.txt'},
            {'IDFL': 'file002', 'dt': datetime(2024, 1, 16), 'filename': 'test2.txt'}
        ]
        mock_cursor.fetchall.return_value = expected_files
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        start_date = datetime(2024, 1, 15)
        end_date = datetime(2024, 1, 16)
        result = db.get_files_by_date_range(start_date, end_date)
        
        assert result == expected_files
        mock_cursor.execute.assert_called_once()
        mock_logger.log_system_info.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_get_migration_statistics(self, mock_pool_class, mock_config, mock_logger):
        """Тест получения статистики миграции."""
        mock_pool = Mock()
        mock_connection = Mock()
        mock_cursor = Mock()
        
        expected_stats = {
            'total_files': 100,
            'moved_files': 50,
            'unmoved_files': 50,
            'earliest_file_date': datetime(2024, 1, 1),
            'latest_file_date': datetime(2024, 1, 31)
        }
        mock_cursor.fetchall.return_value = [expected_stats]
        mock_connection.cursor.return_value = mock_cursor
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        result = db.get_migration_statistics()
        
        assert result == expected_stats
        mock_cursor.execute.assert_called_once()
        mock_logger.log_system_info.assert_called_once_with("Получена статистика миграции")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_cleanup_old_connections(self, mock_pool_class, mock_config, mock_logger):
        """Тест очистки старых соединений."""
        mock_pool = Mock()
        mock_pool.pool_size = 3
        mock_connection = Mock()
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        db.cleanup_old_connections()
        
        # Проверяем, что get_connection вызван 3 раза (по количеству соединений в пуле)
        assert mock_pool.get_connection.call_count == 3
        assert mock_connection.close.call_count == 3
        mock_logger.log_system_info.assert_called_once_with("Очистка старых соединений выполнена")
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_close(self, mock_pool_class, mock_config, mock_logger):
        """Тест закрытия соединений."""
        mock_pool = Mock()
        mock_pool.pool_size = 2
        mock_connection = Mock()
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        db = Database(mock_config, mock_logger)
        db.close()
        
        assert db.connection_pool is None
        mock_logger.log_database_disconnected.assert_called_once()
    
    @patch('src.db.pooling.MySQLConnectionPool')
    def test_context_manager(self, mock_pool_class, mock_config, mock_logger):
        """Тест контекстного менеджера."""
        mock_pool = Mock()
        mock_pool.pool_size = 1
        mock_connection = Mock()
        mock_pool.get_connection.return_value = mock_connection
        mock_pool_class.return_value = mock_pool
        
        with Database(mock_config, mock_logger) as db:
            assert db.connection_pool is not None
        
        # После выхода из контекста соединения должны быть закрыты
        mock_logger.log_database_disconnected.assert_called_once()


class TestCreateDatabase:
    """Тесты для функции create_database."""
    
    def test_create_database(self):
        """Тест создания объекта базы данных."""
        mock_config = Mock(spec=DatabaseConfig)
        mock_logger = Mock(spec=FileMigratorLogger)
        
        with patch('src.db.Database') as mock_db_class:
            mock_db_instance = Mock()
            mock_db_class.return_value = mock_db_instance
            
            result = create_database(mock_config, mock_logger)
            
            assert result == mock_db_instance
            mock_db_class.assert_called_once_with(mock_config, mock_logger)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
