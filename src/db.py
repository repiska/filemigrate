"""
Модуль для работы с базой данных.

Обеспечивает подключение к MySQL, выполнение запросов и управление
таблицей repl_AV_ATF для миграции файлов.
"""

import mysql.connector
from mysql.connector import Error, pooling
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import time
from pathlib import Path

try:
    from .config_loader import DatabaseConfig
    from .logger import FileMigratorLogger
except ImportError:
    from config_loader import DatabaseConfig
    from logger import FileMigratorLogger


class DatabaseConnectionError(Exception):
    """Исключение для ошибок подключения к БД."""
    pass


class DatabaseQueryError(Exception):
    """Исключение для ошибок выполнения запросов."""
    pass


class Database:
    """Класс для работы с базой данных MySQL."""
    
    def __init__(self, config: DatabaseConfig, logger: FileMigratorLogger):
        """
        Инициализация подключения к базе данных.
        
        Args:
            config: Конфигурация подключения к БД
            logger: Логгер для записи операций
        """
        self.config = config
        self.logger = logger
        self.connection_pool: Optional[pooling.MySQLConnectionPool] = None
        self._connection: Optional[mysql.connector.connection.MySQLConnection] = None
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
        """
        Получает соединение из пула.
        
        Returns:
            MySQLConnection: Соединение с БД
            
        Raises:
            DatabaseConnectionError: Если не удалось получить соединение
        """
        try:
            if self.connection_pool is None:
                raise DatabaseConnectionError("Пул соединений не инициализирован")
            
            connection = self.connection_pool.get_connection()
            return connection
            
        except Error as e:
            self.logger.log_database_error("get_connection", e)
            raise DatabaseConnectionError(f"Ошибка получения соединения: {e}")
    
    def _execute_query(self, query: str, params: Tuple = None, fetch: bool = False) -> Optional[List[Dict]]:
        """
        Выполняет SQL запрос.
        
        Args:
            query: SQL запрос
            params: Параметры запроса
            fetch: Возвращать ли результат
            
        Returns:
            List[Dict] или None: Результат запроса
            
        Raises:
            DatabaseQueryError: Если произошла ошибка выполнения запроса
        """
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
        """
        Тестирует подключение к базе данных.
        
        Returns:
            bool: True если подключение успешно
        """
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
        """
        Получает список файлов для миграции.
        
        Args:
            batch_size: Размер батча
            
        Returns:
            List[Dict]: Список файлов для миграции
        """
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
    
    def get_total_files_count(self) -> int:
        """
        Получает общее количество файлов в таблице.
        
        Returns:
            int: Общее количество файлов
        """
        query = "SELECT COUNT(*) as total FROM repl_AV_ATF"
        
        try:
            result = self._execute_query(query, fetch=True)
            total = result[0]['total'] if result else 0
            self.logger.log_system_info(f"Общее количество файлов в БД: {total}")
            return total
            
        except DatabaseQueryError as e:
            self.logger.log_database_error("get_total_files_count", e)
            raise
    
    def get_unmoved_files_count(self) -> int:
        """
        Получает количество файлов, которые еще не перемещены.
        
        Returns:
            int: Количество не перемещенных файлов
        """
        query = "SELECT COUNT(*) as total FROM repl_AV_ATF WHERE ismooved = 0"
        
        try:
            result = self._execute_query(query, fetch=True)
            total = result[0]['total'] if result else 0
            self.logger.log_system_info(f"Количество не перемещенных файлов: {total}")
            return total
            
        except DatabaseQueryError as e:
            self.logger.log_database_error("get_unmoved_files_count", e)
            raise
    
    def mark_file_moved(self, idfl: str, dtmoove: datetime = None) -> bool:
        """
        Отмечает файл как перемещенный.
        
        Args:
            idfl: Идентификатор файла
            dtmoove: Дата и время перемещения (по умолчанию текущее время)
            
        Returns:
            bool: True если операция успешна
        """
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
    
    def insert_new_file(self, idfl: str, filename: str, dt: datetime = None) -> bool:
        """
        Добавляет новый файл в таблицу.
        
        Args:
            idfl: Идентификатор файла
            filename: Имя файла
            dt: Дата помещения в базу (по умолчанию текущее время)
            
        Returns:
            bool: True если операция успешна
        """
        if dt is None:
            dt = datetime.now()
        
        query = """
        INSERT INTO repl_AV_ATF (IDFL, dt, filename, ismooved, created_at, updated_at)
        VALUES (%s, %s, %s, 0, NOW(), NOW())
        ON DUPLICATE KEY UPDATE
        filename = VALUES(filename),
        dt = VALUES(dt),
        updated_at = NOW()
        """
        
        try:
            self._execute_query(query, (idfl, dt, filename))
            self.logger.log_system_info(f"Добавлен новый файл: {idfl} ({filename})")
            return True
            
        except DatabaseQueryError as e:
            self.logger.log_database_error("insert_new_file", e)
            return False
    
    def get_file_metadata(self, idfl: str) -> Optional[Dict]:
        """
        Получает метаданные файла по идентификатору.
        
        Args:
            idfl: Идентификатор файла
            
        Returns:
            Dict или None: Метаданные файла
        """
        query = """
        SELECT IDFL, dt, filename, ismooved, dtmoove, created_at, updated_at
        FROM repl_AV_ATF 
        WHERE IDFL = %s
        """
        
        try:
            result = self._execute_query(query, (idfl,), fetch=True)
            if result:
                self.logger.log_system_info(f"Получены метаданные для файла {idfl}")
                return result[0]
            else:
                self.logger.log_warning(f"Файл {idfl} не найден в БД")
                return None
                
        except DatabaseQueryError as e:
            self.logger.log_database_error("get_file_metadata", e)
            raise
    
    def get_files_by_date_range(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Получает файлы в указанном диапазоне дат.
        
        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            
        Returns:
            List[Dict]: Список файлов в диапазоне дат
        """
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
    
    def get_migration_statistics(self) -> Dict:
        """
        Получает статистику миграции.
        
        Returns:
            Dict: Статистика миграции
        """
        query = """
        SELECT 
            COUNT(*) as total_files,
            SUM(CASE WHEN ismooved = 1 THEN 1 ELSE 0 END) as moved_files,
            SUM(CASE WHEN ismooved = 0 THEN 1 ELSE 0 END) as unmoved_files,
            MIN(dt) as earliest_file_date,
            MAX(dt) as latest_file_date,
            MIN(CASE WHEN ismooved = 1 THEN dtmoove END) as first_migration_date,
            MAX(CASE WHEN ismooved = 1 THEN dtmoove END) as last_migration_date
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
    
    def cleanup_old_connections(self) -> None:
        """Очищает старые соединения из пула."""
        try:
            if self.connection_pool:
                # Принудительно закрываем все соединения
                for _ in range(self.connection_pool.pool_size):
                    try:
                        conn = self.connection_pool.get_connection()
                        conn.close()
                    except:
                        pass
                
                self.logger.log_system_info("Очистка старых соединений выполнена")
                
        except Exception as e:
            self.logger.log_database_error("cleanup_old_connections", e)
    
    def close(self) -> None:
        """Закрывает все соединения и очищает ресурсы."""
        try:
            if self.connection_pool:
                self.cleanup_old_connections()
                self.connection_pool = None
            
            self.logger.log_database_disconnected()
            
        except Exception as e:
            self.logger.log_database_error("close", e)
    
    def __enter__(self):
        """Поддержка контекстного менеджера."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Поддержка контекстного менеджера."""
        self.close()


def create_database(config: DatabaseConfig, logger: FileMigratorLogger) -> Database:
    """
    Удобная функция для создания объекта базы данных.
    
    Args:
        config: Конфигурация подключения к БД
        logger: Логгер
        
    Returns:
        Database: Объект базы данных
    """
    return Database(config, logger)


if __name__ == "__main__":
    # Тестирование модуля
    try:
        from config_loader import load_config
        from logger import setup_logger, FileMigratorLogger
        
        # Загружаем конфигурацию
        app_config = load_config()
        
        # Настраиваем логгер
        logger = FileMigratorLogger(app_config.logging)
        
        # Создаем объект БД
        db = Database(app_config.database, logger)
        
        # Тестируем подключение
        if db.test_connection():
            print("✅ Подключение к БД успешно!")
            
            # Получаем статистику
            stats = db.get_migration_statistics()
            print(f"📊 Статистика: {stats}")
            
            # Получаем количество файлов
            total = db.get_total_files_count()
            unmoved = db.get_unmoved_files_count()
            print(f"📁 Всего файлов: {total}, не перемещено: {unmoved}")
            
            # Получаем файлы для миграции
            files = db.get_files_to_move(5)
            print(f"📦 Файлы для миграции: {len(files)}")
            for file_info in files[:3]:  # Показываем первые 3
                print(f"   • {file_info['IDFL']} - {file_info['filename']}")
            
        else:
            print("❌ Ошибка подключения к БД")
        
        # Закрываем соединения
        db.close()
        
    except Exception as e:
        print(f"❌ Ошибка тестирования БД: {e}")
        import traceback
        traceback.print_exc()
