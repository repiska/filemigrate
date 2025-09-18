"""
Модуль для операций с файловой системой.

Обеспечивает перемещение файлов, чтение и запись с поддержкой
новой структуры каталогов по датам (YYYYMMDD).
"""

import os
import shutil
from pathlib import Path
from typing import Optional, Union, List
from datetime import datetime
import hashlib

try:
    from .config_loader import PathsConfig
    from .logger import FileMigratorLogger
except ImportError:
    from config_loader import PathsConfig
    from logger import FileMigratorLogger


class FileOperationError(Exception):
    """Исключение для ошибок операций с файлами."""
    pass


class FileNotFoundError(FileOperationError):
    """Исключение для случая, когда файл не найден."""
    pass


class FileOps:
    """Класс для операций с файловой системой."""
    
    def __init__(self, paths_config: PathsConfig, logger: FileMigratorLogger):
        """
        Инициализация операций с файлами.
        
        Args:
            paths_config: Конфигурация путей
            logger: Логгер для записи операций
        """
        self.paths_config = paths_config
        self.logger = logger
        self.base_path = Path(paths_config.file_path)
        self.new_base_path = Path(paths_config.new_file_path)
        
        # Создаем базовые каталоги если они не существуют
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
        """
        Получает путь к каталогу по дате в формате YYYYMMDD.
        
        Args:
            dt: Дата для создания пути
            
        Returns:
            Path: Путь к каталогу по дате
        """
        date_str = dt.strftime("%Y%m%d")
        return self.new_base_path / date_str
    
    def _ensure_date_directory_exists(self, dt: datetime) -> Path:
        """
        Создает каталог по дате если он не существует.
        
        Args:
            dt: Дата для создания каталога
            
        Returns:
            Path: Путь к созданному каталогу
        """
        date_dir = self._get_date_directory(dt)
        try:
            date_dir.mkdir(parents=True, exist_ok=True)
            return date_dir
        except Exception as e:
            self.logger.log_database_error("create_date_directory", e)
            raise FileOperationError(f"Ошибка создания каталога по дате: {e}")
    
    def move_file(self, idfl: str, dt: datetime) -> Path:
        """
        Перемещает файл в новую структуру каталогов по дате.
        
        Args:
            idfl: Идентификатор файла
            dt: Дата для создания структуры каталогов
            
        Returns:
            Path: Путь к перемещенному файлу
            
        Raises:
            FileNotFoundError: Если исходный файл не найден
            FileOperationError: Если произошла ошибка при перемещении
        """
        source_path = self.base_path / idfl
        
        if not source_path.exists():
            error_msg = f"Исходный файл не найден: {source_path}"
            self.logger.log_file_error(idfl, FileNotFoundError(error_msg))
            raise FileNotFoundError(error_msg)
        
        try:
            # Создаем каталог по дате
            target_dir = self._ensure_date_directory_exists(dt)
            target_path = target_dir / idfl
            
            # Проверяем, не существует ли уже файл в целевом каталоге
            if target_path.exists():
                # Создаем уникальное имя файла
                target_path = self._get_unique_filename(target_dir, idfl)
            
            # Перемещаем файл
            shutil.move(str(source_path), str(target_path))
            
            self.logger.log_file_moved(idfl, source_path, target_path)
            self.logger.log_file_operation("move", target_path, True)
            
            return target_path
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            raise FileOperationError(f"Ошибка перемещения файла {idfl}: {e}")
    
    def _get_unique_filename(self, directory: Path, filename: str) -> Path:
        """
        Получает уникальное имя файла в каталоге.
        
        Args:
            directory: Каталог для проверки
            filename: Исходное имя файла
            
        Returns:
            Path: Уникальное имя файла
        """
        base_path = directory / filename
        if not base_path.exists():
            return base_path
        
        # Добавляем суффикс с номером
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
    
    def read_file(self, idfl: str, ismooved: bool, dt: datetime) -> bytes:
        """
        Читает файл по старой или новой схеме в зависимости от статуса.
        
        Args:
            idfl: Идентификатор файла
            ismooved: Признак перемещения файла
            dt: Дата файла (для новой схемы)
            
        Returns:
            bytes: Содержимое файла
            
        Raises:
            FileNotFoundError: Если файл не найден
            FileOperationError: Если произошла ошибка при чтении
        """
        try:
            if ismooved:
                # Читаем по новой схеме (из каталога по дате)
                date_dir = self._get_date_directory(dt)
                file_path = date_dir / idfl
            else:
                # Читаем по старой схеме (из базового каталога)
                file_path = self.base_path / idfl
            
            if not file_path.exists():
                error_msg = f"Файл не найден: {file_path}"
                self.logger.log_file_error(idfl, FileNotFoundError(error_msg))
                raise FileNotFoundError(error_msg)
            
            with open(file_path, 'rb') as f:
                content = f.read()
            
            self.logger.log_file_operation("read", file_path, True)
            return content
            
        except FileNotFoundError:
            raise
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            raise FileOperationError(f"Ошибка чтения файла {idfl}: {e}")
    
    def write_file(self, idfl: str, filename: str, data: bytes, dt: datetime = None) -> Path:
        """
        Записывает новый файл сразу в новую структуру каталогов.
        
        Args:
            idfl: Идентификатор файла
            filename: Имя файла
            data: Данные для записи
            dt: Дата для создания структуры каталогов (по умолчанию текущая)
            
        Returns:
            Path: Путь к записанному файлу
            
        Raises:
            FileOperationError: Если произошла ошибка при записи
        """
        if dt is None:
            dt = datetime.now()
        
        try:
            # Создаем каталог по дате
            target_dir = self._ensure_date_directory_exists(dt)
            target_path = target_dir / idfl
            
            # Проверяем, не существует ли уже файл
            if target_path.exists():
                target_path = self._get_unique_filename(target_dir, idfl)
            
            # Записываем файл
            with open(target_path, 'wb') as f:
                f.write(data)
            
            self.logger.log_file_operation("write", target_path, True)
            self.logger.log_system_info(f"Новый файл записан: {idfl} -> {target_path}")
            
            return target_path
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            raise FileOperationError(f"Ошибка записи файла {idfl}: {e}")
    
    def file_exists(self, idfl: str, ismooved: bool, dt: datetime) -> bool:
        """
        Проверяет существование файла по старой или новой схеме.
        
        Args:
            idfl: Идентификатор файла
            ismooved: Признак перемещения файла
            dt: Дата файла (для новой схемы)
            
        Returns:
            bool: True если файл существует
        """
        try:
            if ismooved:
                # Проверяем по новой схеме
                date_dir = self._get_date_directory(dt)
                file_path = date_dir / idfl
            else:
                # Проверяем по старой схеме
                file_path = self.base_path / idfl
            
            exists = file_path.exists()
            self.logger.log_system_info(f"Проверка файла {idfl}: {'существует' if exists else 'не найден'}")
            return exists
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            return False
    
    def get_file_size(self, idfl: str, ismooved: bool, dt: datetime) -> Optional[int]:
        """
        Получает размер файла в байтах.
        
        Args:
            idfl: Идентификатор файла
            ismooved: Признак перемещения файла
            dt: Дата файла (для новой схемы)
            
        Returns:
            int или None: Размер файла в байтах или None если файл не найден
        """
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
        """
        Получает хеш файла для проверки целостности.
        
        Args:
            idfl: Идентификатор файла
            ismooved: Признак перемещения файла
            dt: Дата файла (для новой схемы)
            algorithm: Алгоритм хеширования (md5, sha1, sha256)
            
        Returns:
            str или None: Хеш файла или None если файл не найден
        """
        try:
            if ismooved:
                date_dir = self._get_date_directory(dt)
                file_path = date_dir / idfl
            else:
                file_path = self.base_path / idfl
            
            if not file_path.exists():
                return None
            
            # Выбираем алгоритм хеширования
            if algorithm == 'md5':
                hasher = hashlib.md5()
            elif algorithm == 'sha1':
                hasher = hashlib.sha1()
            elif algorithm == 'sha256':
                hasher = hashlib.sha256()
            else:
                raise ValueError(f"Неподдерживаемый алгоритм: {algorithm}")
            
            # Читаем файл и вычисляем хеш
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            
            file_hash = hasher.hexdigest()
            self.logger.log_system_info(f"Хеш файла {idfl} ({algorithm}): {file_hash}")
            return file_hash
            
        except Exception as e:
            self.logger.log_file_error(idfl, e)
            return None
    
    def list_files_in_date_directory(self, dt: datetime) -> List[Path]:
        """
        Получает список файлов в каталоге по дате.
        
        Args:
            dt: Дата для получения списка файлов
            
        Returns:
            List[Path]: Список путей к файлам
        """
        try:
            date_dir = self._get_date_directory(dt)
            if not date_dir.exists():
                return []
            
            files = [f for f in date_dir.iterdir() if f.is_file()]
            self.logger.log_system_info(f"Файлов в каталоге {date_dir}: {len(files)}")
            return files
            
        except Exception as e:
            self.logger.log_database_error("list_files_in_date_directory", e)
            return []
    
    def list_unmoved_files(self) -> List[Path]:
        """
        Получает список файлов в базовом каталоге (не перемещенных).
        
        Returns:
            List[Path]: Список путей к не перемещенным файлам
        """
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
        """
        Удаляет пустые каталоги в новой структуре.
        
        Returns:
            int: Количество удаленных каталогов
        """
        try:
            removed_count = 0
            
            if not self.new_base_path.exists():
                return 0
            
            # Проходим по всем подкаталогам (каталоги по датам)
            for date_dir in self.new_base_path.iterdir():
                if date_dir.is_dir():
                    try:
                        # Проверяем, пуст ли каталог
                        if not any(date_dir.iterdir()):
                            date_dir.rmdir()
                            removed_count += 1
                            self.logger.log_system_info(f"Удален пустой каталог: {date_dir}")
                    except OSError:
                        # Каталог не пуст или есть ошибка доступа
                        pass
            
            if removed_count > 0:
                self.logger.log_system_info(f"Удалено пустых каталогов: {removed_count}")
            
            return removed_count
            
        except Exception as e:
            self.logger.log_database_error("cleanup_empty_directories", e)
            return 0
    
    def get_storage_statistics(self) -> dict:
        """
        Получает статистику использования дискового пространства.
        
        Returns:
            dict: Статистика использования пространства
        """
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
            
            # Статистика не перемещенных файлов
            if self.base_path.exists():
                unmoved_files = self.list_unmoved_files()
                stats['unmoved_files_count'] = len(unmoved_files)
                stats['unmoved_files_size'] = sum(f.stat().st_size for f in unmoved_files)
            
            # Статистика перемещенных файлов
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


def create_file_ops(paths_config: PathsConfig, logger: FileMigratorLogger) -> FileOps:
    """
    Удобная функция для создания объекта операций с файлами.
    
    Args:
        paths_config: Конфигурация путей
        logger: Логгер
        
    Returns:
        FileOps: Объект операций с файлами
    """
    return FileOps(paths_config, logger)


if __name__ == "__main__":
    # Тестирование модуля
    try:
        from config_loader import load_config
        from logger import FileMigratorLogger
        
        # Загружаем конфигурацию
        app_config = load_config()
        
        # Настраиваем логгер
        logger = FileMigratorLogger(app_config.logging)
        
        # Создаем объект операций с файлами
        file_ops = FileOps(app_config.paths, logger)
        
        print("✅ FileOps успешно инициализирован!")
        
        # Получаем статистику
        stats = file_ops.get_storage_statistics()
        print(f"📊 Статистика хранилища: {stats}")
        
        # Список не перемещенных файлов
        unmoved_files = file_ops.list_unmoved_files()
        print(f"📁 Не перемещенных файлов: {len(unmoved_files)}")
        for file_path in unmoved_files[:5]:  # Показываем первые 5
            print(f"   • {file_path.name}")
        
        # Тестируем чтение файла
        if unmoved_files:
            test_file = unmoved_files[0]
            try:
                content = file_ops.read_file(test_file.name, False, datetime.now())
                print(f"📖 Файл {test_file.name} прочитан, размер: {len(content)} байт")
            except Exception as e:
                print(f"❌ Ошибка чтения файла: {e}")
        
    except Exception as e:
        print(f"❌ Ошибка тестирования FileOps: {e}")
        import traceback
        traceback.print_exc()
