-- Используем созданную базу данных
USE FileMigratorTest;

-- Создание таблицы repl_AV_ATF
CREATE TABLE repl_AV_ATF (
    IDFL VARCHAR(50) PRIMARY KEY,            -- Системное имя файла
    dt DATETIME NOT NULL,                    -- Дата помещения в базу
    filename VARCHAR(255) NOT NULL,          -- Пользовательское имя файла
    ismooved BOOLEAN DEFAULT FALSE,          -- Признак перемещения
    dtmoove DATETIME NULL,                   -- Дата и время перемещения
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,  -- Дата создания записи
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  -- Дата обновления записи
);

-- Создание индексов для оптимизации
CREATE INDEX IX_repl_AV_ATF_dt ON repl_AV_ATF(dt);
CREATE INDEX IX_repl_AV_ATF_ismooved ON repl_AV_ATF(ismooved);
CREATE INDEX IX_repl_AV_ATF_dtmoove ON repl_AV_ATF(dtmoove);

-- Вставка тестовых данных
INSERT INTO repl_AV_ATF (IDFL, dt, filename) VALUES
('file001', '2024-01-15 10:30:00', 'document1.pdf'),
('file002', '2024-01-15 14:20:00', 'image1.jpg'),
('file003', '2024-01-16 09:15:00', 'spreadsheet1.xlsx'),
('file004', '2024-01-16 11:45:00', 'presentation1.pptx'),
('file005', '2024-01-17 16:30:00', 'text1.txt'),
('file006', '2024-01-17 18:00:00', 'archive1.zip'),
('file007', '2024-01-18 08:45:00', 'video1.mp4'),
('file008', '2024-01-18 12:15:00', 'audio1.mp3'),
('file009', '2024-01-19 13:20:00', 'code1.py'),
('file010', '2024-01-19 15:10:00', 'config1.ini');
