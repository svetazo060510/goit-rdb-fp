-- =================================================================================
-- КРОК 1. СТВОРЕННЯ СХЕМИ
-- Використовуємо IF NOT EXISTS, щоб не було помилки при повторному запуску
-- =================================================================================

CREATE SCHEMA IF NOT EXISTS pandemic;
USE pandemic;

-- Рахуємо загальну кількість записів у початковій таблиці
SELECT COUNT(*) AS total_records_imported 
FROM infectious_cases;

-- =================================================================================
-- КРОК 2. НОРМАЛІЗАЦІЯ ДАНИХ
-- Опис: виносимо країни в окрему таблицю (3NF) та створюємо зв'язки.
-- Скрипт ідемпотентний: можна запускати багато разів без дублів.
-- =================================================================================

-- 1. Створюємо довідник країн
CREATE TABLE IF NOT EXISTS countries (
    id INT AUTO_INCREMENT PRIMARY KEY,
    entity VARCHAR(255) NOT NULL,
    code VARCHAR(10),
    UNIQUE KEY unique_entity (entity) 
);

-- Наповнюємо довідник унікальними країнами
INSERT IGNORE INTO countries (entity, code)
SELECT DISTINCT Entity, Code 
FROM infectious_cases
WHERE Entity IS NOT NULL;

-- 2. Створюємо основну нормалізовану таблицю
CREATE TABLE IF NOT EXISTS infectious_cases_normalized (
    id INT AUTO_INCREMENT PRIMARY KEY,
    country_id INT,
    year INT,
    number_yaws FLOAT,
    polio_cases FLOAT,
    cases_guinea_worm FLOAT,
    number_rabies FLOAT,
    number_malaria FLOAT,
    number_hiv FLOAT,
    number_tuberculosis FLOAT,
    number_smallpox FLOAT,
    number_cholera_cases FLOAT,
    FOREIGN KEY (country_id) REFERENCES countries(id),
    UNIQUE KEY unique_record (country_id, year) -- Захист від повторного запису тих самих даних
);

-- 3. Переносимо дані з початкової таблиці (infectious_cases)
INSERT IGNORE INTO infectious_cases_normalized (
    country_id, year, number_yaws, polio_cases, cases_guinea_worm, 
    number_rabies, number_malaria, number_hiv, number_tuberculosis, 
    number_smallpox, number_cholera_cases
)
SELECT 
    c.id, 
    ic.Year, 
    ic.Number_yaws, 
    ic.polio_cases, 
    ic.cases_guinea_worm, 
    ic.Number_rabies, 
    ic.Number_malaria, 
    ic.Number_hiv, 
    ic.Number_tuberculosis, 
    ic.Number_smallpox, 
    ic.Number_cholera_cases
FROM infectious_cases ic
JOIN countries c ON ic.Entity = c.entity;

-- 4. Фінальна перевірка (виводимо 10 рядків)
SELECT icn.*, c.entity 
FROM infectious_cases_normalized icn
JOIN countries c ON icn.country_id = c.id
LIMIT 10;

-- 5. Перевіряємо скільки рядків у нормалізованій таблиці
SELECT COUNT(*) AS normalized_data_count 
FROM infectious_cases_normalized;

-- =================================================================================
-- КРОК 3. АНАЛІЗ ДАНИХ (Сказ / Number_rabies)
-- Розрахувати статистику захворюваності на сказ для кожної країни.
-- Опис: Рахуємо середнє, мінімальне, максимальне значення та суму.
-- Фільтруємо порожні значення та виводимо ТОП-10 за середнім показником.
-- =================================================================================

SELECT 
    c.entity,
    c.code,
    AVG(icn.number_rabies) AS average_rabies,
    MIN(icn.number_rabies) AS min_rabies,
    MAX(icn.number_rabies) AS max_rabies,
    SUM(icn.number_rabies) AS total_rabies
FROM infectious_cases_normalized icn
JOIN countries c ON icn.country_id = c.id
-- Фільтруємо порожні значення. В SQL після імпорту числових даних 
-- порожні клітинки зазвичай стають NULL.
WHERE icn.number_rabies IS NOT NULL AND icn.number_rabies <> 0
GROUP BY c.id, c.entity, c.code
ORDER BY average_rabies DESC
LIMIT 10;

-- =================================================================================
-- КРОК 4. ПОБУДОВА КОЛОНОК ІЗ ДАТАМИ ТА РІЗНИЦЕЮ В РОКАХ
-- =================================================================================

SELECT 
    year,
    -- 1. Створюємо дату 1 січня для кожного року
    -- Функція MAKEDATE(year, day_of_year)
    MAKEDATE(year, 1) AS first_january_date,
    
    -- 2. Отримуємо поточну системну дату
    CURDATE() AS current_date_column,
    
    -- 3. Обчислюємо різницю в роках
    TIMESTAMPDIFF(YEAR, MAKEDATE(year, 1), CURDATE()) AS year_difference
FROM infectious_cases_normalized
-- Групуємо за роком, щоб побачити результат для кожного унікального року без повторів.
GROUP BY year;

-- =================================================================================
-- КРОК 5. СТВОРЕННЯ ВЛАСНОЇ ФУНКЦІЇ
-- Мета: Написати функцію, яка автоматизує розрахунок різниці в роках.
-- =================================================================================

-- Видаляємо функцію, якщо вона вже була створена раніше (для ідемпотентності)
DROP FUNCTION IF EXISTS calculate_year_diff;

DELIMITER //

CREATE FUNCTION calculate_year_diff(input_year INT) 
RETURNS INT
DETERMINISTIC 
NO SQL
BEGIN
    DECLARE result INT;
    -- Логіка: створюємо дату 1 січня та рахуємо різницю з сьогоднішнім днем
    SET result = TIMESTAMPDIFF(YEAR, MAKEDATE(input_year, 1), CURDATE());
    RETURN result;
END //

DELIMITER ;

-- Використовуємо функцію на даних
SELECT 
    year,
    calculate_year_diff(year) AS years_passed
FROM infectious_cases_normalized
GROUP BY year;