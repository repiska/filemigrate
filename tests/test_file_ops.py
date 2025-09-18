"""
Тесты для модуля file_ops.py
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, mock_open
import os

from src.file_ops import FileOps, FileOperationError, FileNotFoundError, create_file_ops
from src.config_loader import PathsConfig
from src.logger import FileMigratorLogger


class TestFileOps:
    """Тесты для класса FileOps."""
    
    @pytest.fixture
    def temp_dir(self):
        """Создает временную директорию для тестов."""
        temp_path = tempfile.mkdtemp()
        yield Path(temp_path)
        shutil.rmtree(temp_path, ignore_errors=True)
    
    @pytest.fixture
    def mock_paths_config(self, temp_dir):
        """Создает мок конфигурации путей."""
        return PathsConfig(
            file_path=temp_dir / "base",
            new_file_path=temp_dir / "new"
        )
    
    @pytest.fixture
    def mock_logger(self):
        """Создает мок логгера."""
        logger = Mock(spec=FileMigratorLogger)
        return logger
    
    @pytest.fixture
    def file_ops(self, mock_paths_config, mock_logger):
        """Создает объект FileOps для тестов."""
        return FileOps(mock_paths_config, mock_logger)
    
    def test_file_ops_initialization(self, mock_paths_config, mock_logger, temp_dir):
        """Тест инициализации FileOps."""
        file_ops = FileOps(mock_paths_config, mock_logger)
        
        assert file_ops.paths_config == mock_paths_config
        assert file_ops.logger == mock_logger
        assert file_ops.base_path == temp_dir / "base"
        assert file_ops.new_base_path == temp_dir / "new"
        
        # Проверяем, что каталоги созданы
        assert file_ops.base_path.exists()
        assert file_ops.new_base_path.exists()
        
        mock_logger.log_system_info.assert_called()
    
    def test_get_date_directory(self, file_ops):
        """Тест получения каталога по дате."""
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._get_date_directory(test_date)
        
        expected_path = file_ops.new_base_path / "20240115"
        assert date_dir == expected_path
    
    def test_ensure_date_directory_exists(self, file_ops):
        """Тест создания каталога по дате."""
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        
        assert date_dir.exists()
        assert date_dir == file_ops.new_base_path / "20240115"
    
    def test_move_file_success(self, file_ops, temp_dir):
        """Тест успешного перемещения файла."""
        # Создаем тестовый файл
        test_file = file_ops.base_path / "test_file.txt"
        test_file.write_text("test content")
        
        test_date = datetime(2024, 1, 15, 10, 30)
        result_path = file_ops.move_file("test_file.txt", test_date)
        
        # Проверяем, что файл перемещен
        assert result_path.exists()
        assert result_path == file_ops.new_base_path / "20240115" / "test_file.txt"
        assert not test_file.exists()
        
        # Проверяем содержимое
        assert result_path.read_text() == "test content"
        
        # Проверяем логирование
        file_ops.logger.log_file_moved.assert_called_once()
        file_ops.logger.log_file_operation.assert_called_once()
    
    def test_move_file_not_found(self, file_ops):
        """Тест перемещения несуществующего файла."""
        test_date = datetime(2024, 1, 15, 10, 30)
        
        with pytest.raises(FileNotFoundError):
            file_ops.move_file("nonexistent.txt", test_date)
        
        file_ops.logger.log_file_error.assert_called_once()
    
    def test_move_file_with_existing_target(self, file_ops, temp_dir):
        """Тест перемещения файла когда целевой файл уже существует."""
        # Создаем исходный файл
        source_file = file_ops.base_path / "test_file.txt"
        source_file.write_text("original content")
        
        # Создаем каталог по дате и файл в нем
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        existing_file = date_dir / "test_file.txt"
        existing_file.write_text("existing content")
        
        # Перемещаем файл
        result_path = file_ops.move_file("test_file.txt", test_date)
        
        # Проверяем, что создан файл с уникальным именем
        assert result_path != existing_file
        assert result_path.exists()
        assert result_path.read_text() == "original content"
        assert existing_file.read_text() == "existing content"
    
    def test_read_file_old_scheme(self, file_ops, temp_dir):
        """Тест чтения файла по старой схеме."""
        # Создаем тестовый файл
        test_file = file_ops.base_path / "test_file.txt"
        test_content = b"test content"
        test_file.write_bytes(test_content)
        
        # Читаем файл
        result = file_ops.read_file("test_file.txt", False, datetime.now())
        
        assert result == test_content
        file_ops.logger.log_file_operation.assert_called_once()
    
    def test_read_file_new_scheme(self, file_ops, temp_dir):
        """Тест чтения файла по новой схеме."""
        # Создаем каталог по дате и файл в нем
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        test_file = date_dir / "test_file.txt"
        test_content = b"test content"
        test_file.write_bytes(test_content)
        
        # Читаем файл
        result = file_ops.read_file("test_file.txt", True, test_date)
        
        assert result == test_content
        file_ops.logger.log_file_operation.assert_called_once()
    
    def test_read_file_not_found(self, file_ops):
        """Тест чтения несуществующего файла."""
        with pytest.raises(FileNotFoundError):
            file_ops.read_file("nonexistent.txt", False, datetime.now())
        
        file_ops.logger.log_file_error.assert_called_once()
    
    def test_write_file_success(self, file_ops, temp_dir):
        """Тест успешной записи файла."""
        test_date = datetime(2024, 1, 15, 10, 30)
        test_content = b"new file content"
        
        result_path = file_ops.write_file("new_file.txt", "new_file.txt", test_content, test_date)
        
        # Проверяем, что файл создан
        assert result_path.exists()
        assert result_path == file_ops.new_base_path / "20240115" / "new_file.txt"
        assert result_path.read_bytes() == test_content
        
        file_ops.logger.log_file_operation.assert_called_once()
        file_ops.logger.log_system_info.assert_called()
    
    def test_write_file_with_existing_target(self, file_ops, temp_dir):
        """Тест записи файла когда целевой файл уже существует."""
        # Создаем каталог по дате и файл в нем
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        existing_file = date_dir / "new_file.txt"
        existing_file.write_text("existing content")
        
        # Записываем новый файл
        test_content = b"new file content"
        result_path = file_ops.write_file("new_file.txt", "new_file.txt", test_content, test_date)
        
        # Проверяем, что создан файл с уникальным именем
        assert result_path != existing_file
        assert result_path.exists()
        assert result_path.read_bytes() == test_content
        assert existing_file.read_text() == "existing content"
    
    def test_file_exists_old_scheme(self, file_ops, temp_dir):
        """Тест проверки существования файла по старой схеме."""
        # Файл не существует
        assert not file_ops.file_exists("nonexistent.txt", False, datetime.now())
        
        # Создаем файл
        test_file = file_ops.base_path / "test_file.txt"
        test_file.write_text("test content")
        
        # Файл существует
        assert file_ops.file_exists("test_file.txt", False, datetime.now())
    
    def test_file_exists_new_scheme(self, file_ops, temp_dir):
        """Тест проверки существования файла по новой схеме."""
        test_date = datetime(2024, 1, 15, 10, 30)
        
        # Файл не существует
        assert not file_ops.file_exists("nonexistent.txt", True, test_date)
        
        # Создаем каталог по дате и файл в нем
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        test_file = date_dir / "test_file.txt"
        test_file.write_text("test content")
        
        # Файл существует
        assert file_ops.file_exists("test_file.txt", True, test_date)
    
    def test_get_file_size(self, file_ops, temp_dir):
        """Тест получения размера файла."""
        # Создаем тестовый файл
        test_file = file_ops.base_path / "test_file.txt"
        test_content = "test content"
        test_file.write_text(test_content)
        
        # Получаем размер
        size = file_ops.get_file_size("test_file.txt", False, datetime.now())
        
        assert size == len(test_content.encode('utf-8'))
        file_ops.logger.log_system_info.assert_called()
    
    def test_get_file_size_not_found(self, file_ops):
        """Тест получения размера несуществующего файла."""
        size = file_ops.get_file_size("nonexistent.txt", False, datetime.now())
        assert size is None
    
    def test_get_file_hash(self, file_ops, temp_dir):
        """Тест получения хеша файла."""
        # Создаем тестовый файл
        test_file = file_ops.base_path / "test_file.txt"
        test_content = "test content"
        test_file.write_text(test_content)
        
        # Получаем хеш
        file_hash = file_ops.get_file_hash("test_file.txt", False, datetime.now(), "md5")
        
        assert file_hash is not None
        assert len(file_hash) == 32  # MD5 хеш имеет длину 32 символа
        file_ops.logger.log_system_info.assert_called()
    
    def test_get_file_hash_not_found(self, file_ops):
        """Тест получения хеша несуществующего файла."""
        file_hash = file_ops.get_file_hash("nonexistent.txt", False, datetime.now(), "md5")
        assert file_hash is None
    
    def test_list_files_in_date_directory(self, file_ops, temp_dir):
        """Тест получения списка файлов в каталоге по дате."""
        test_date = datetime(2024, 1, 15, 10, 30)
        
        # Каталог не существует
        files = file_ops.list_files_in_date_directory(test_date)
        assert files == []
        
        # Создаем каталог и файлы
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        (date_dir / "file1.txt").write_text("content1")
        (date_dir / "file2.txt").write_text("content2")
        
        # Получаем список файлов
        files = file_ops.list_files_in_date_directory(test_date)
        assert len(files) == 2
        assert all(f.is_file() for f in files)
    
    def test_list_unmoved_files(self, file_ops, temp_dir):
        """Тест получения списка не перемещенных файлов."""
        # Создаем файлы в базовом каталоге
        (file_ops.base_path / "file1.txt").write_text("content1")
        (file_ops.base_path / "file2.txt").write_text("content2")
        
        # Получаем список файлов
        files = file_ops.list_unmoved_files()
        assert len(files) == 2
        assert all(f.is_file() for f in files)
    
    def test_cleanup_empty_directories(self, file_ops, temp_dir):
        """Тест очистки пустых каталогов."""
        test_date = datetime(2024, 1, 15, 10, 30)
        
        # Создаем пустой каталог
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        
        # Очищаем пустые каталоги
        removed_count = file_ops.cleanup_empty_directories()
        
        assert removed_count == 1
        assert not date_dir.exists()
        file_ops.logger.log_system_info.assert_called()
    
    def test_get_storage_statistics(self, file_ops, temp_dir):
        """Тест получения статистики хранилища."""
        # Создаем тестовые файлы
        (file_ops.base_path / "file1.txt").write_text("content1")
        (file_ops.base_path / "file2.txt").write_text("content2")
        
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        (date_dir / "moved_file.txt").write_text("moved content")
        
        # Получаем статистику
        stats = file_ops.get_storage_statistics()
        
        assert stats['unmoved_files_count'] == 2
        assert stats['moved_files_count'] == 1
        assert stats['date_directories_count'] == 1
        assert stats['unmoved_files_size'] > 0
        assert stats['moved_files_size'] > 0
        
        file_ops.logger.log_system_info.assert_called()
    
    def test_get_unique_filename(self, file_ops, temp_dir):
        """Тест получения уникального имени файла."""
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        
        # Создаем существующий файл
        existing_file = date_dir / "test.txt"
        existing_file.write_text("existing")
        
        # Получаем уникальное имя
        unique_path = file_ops._get_unique_filename(date_dir, "test.txt")
        
        assert unique_path != existing_file
        assert unique_path.name == "test_1.txt"
    
    def test_get_unique_filename_with_extension(self, file_ops, temp_dir):
        """Тест получения уникального имени файла с расширением."""
        test_date = datetime(2024, 1, 15, 10, 30)
        date_dir = file_ops._ensure_date_directory_exists(test_date)
        
        # Создаем существующий файл
        existing_file = date_dir / "test.txt"
        existing_file.write_text("existing")
        
        # Получаем уникальное имя
        unique_path = file_ops._get_unique_filename(date_dir, "test.txt")
        
        assert unique_path != existing_file
        assert unique_path.name == "test_1.txt"
        assert unique_path.suffix == ".txt"


class TestCreateFileOps:
    """Тесты для функции create_file_ops."""
    
    def test_create_file_ops(self):
        """Тест создания объекта FileOps."""
        mock_paths_config = Mock(spec=PathsConfig)
        mock_logger = Mock(spec=FileMigratorLogger)
        
        with patch('src.file_ops.FileOps') as mock_file_ops_class:
            mock_file_ops_instance = Mock()
            mock_file_ops_class.return_value = mock_file_ops_instance
            
            result = create_file_ops(mock_paths_config, mock_logger)
            
            assert result == mock_file_ops_instance
            mock_file_ops_class.assert_called_once_with(mock_paths_config, mock_logger)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
