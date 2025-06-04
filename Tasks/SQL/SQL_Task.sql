
-- Завдання 1, рішення 1
-- Логіка:

-- 1. Актуальна ціна для кожного кліку:
--    1.1. JOIN із job, щоб мати доступ до поточної ціни j.click_price.
--    1.2. Використовую LEFT JOIN LATERAL, щоб для кожного кліку знайти останню зміну ціни в price_log (date_time <= click_time).
--    1.3. Якщо такий запис відсутній — підставляю поточну ціну з job через COALESCE(pl.click_price, j.click_price).

-- 2. Підсумовую всі застосовані ціни (applied_click_price) — отримую загальний дохід.

WITH clicks_prices AS (
    SELECT
        c.date_time AS click_time,
        c.job_id,
        COALESCE(pl.click_price, j.click_price) AS applied_click_price
    FROM click AS c
    JOIN job AS j ON j.id = c.job_id
    LEFT JOIN LATERAL (
        SELECT click_price
        FROM price_log
        WHERE job_id = c.job_id AND date_time <= c.date_time
        ORDER BY date_time DESC
        LIMIT 1
    ) AS pl
      ON TRUE
)
SELECT SUM(applied_click_price) AS total_revenue
FROM clicks_prices;


-- Завдання 1, рішення 2
-- Логіка:

-- 1. Для кожного кліку підтягуються всі попередні зміни ціни з таблиці price_log (де date_time <= click_time).
-- 2. За допомогою віконної функції ROW_NUMBER() визначаю останній запис про зміну ціни на момент кліку.
-- 3. Якщо такого запису немає — підставляю поточну ціну з таблиці job через COALESCE.
-- 4. Обчислюю загальний дохід, підсумовуючи застосовані ціни для кожного кліку (одна ціна на клік).

WITH clicks_prices AS (
    SELECT
        c.date_time AS click_time,
        c.job_id AS job_id,
        j.click_price AS fallback_price,
        pl.click_price AS logged_price,
        ROW_NUMBER() OVER (PARTITION BY c.job_id, c.date_time ORDER BY pl.date_time DESC) AS row_num
    FROM click AS c
    JOIN job AS j ON j.id = c.job_id
    LEFT JOIN price_log AS pl ON pl.job_id = c.job_id AND pl.date_time <= c.date_time
)
SELECT SUM(COALESCE(logged_price, fallback_price)) AS total_revenue
FROM clicks_prices
WHERE row_num = 1;


-- Завдання 2
-- Логіка:

-- 1. Побудова дерева категорій:
--    Рекурсивно піднімаю кожну category_id до її root через parent_category_id.

-- 2. Визначення батьківських категорій:
--    Вибираю категорії, у яких parent_category_id IS NULL.

-- 3. Розрахунок доходу:
--    Для кожного кліку шукаю актуальну ціну:
--    останній запис у price_log до моменту кліку,
--    якщо запис відсутній - беру актуальну ціну з таблиці job.

-- 4. Агрегація доходу:
--    Підсумовую дохід по category_id, потім маплю до батьківських категорій.

-- 5. Підсумковий результат:
--    Виводжу дохід по батьківським категоріям.

WITH RECURSIVE category_tree AS (
    SELECT 
        id AS category_id,
        child_category_name AS child_category_name,
        parent_category_id AS parent_category_id,
        parent_categor_name AS parent_category_name,
        id AS original_category_id
    FROM category

    UNION ALL

    SELECT 
        c.id AS category_id,
        c.child_category_name AS child_category_name,
        c.parent_category_id AS parent_category_id,
        c.parent_categor_name AS parent_category_name,
        ct.original_category_id AS original_category_id
    FROM category AS c
    JOIN category_tree AS ct ON c.id = ct.parent_category_id
),

final_categories AS (
    SELECT 
        original_category_id AS category_id,
        category_id AS root_category_id,
        child_category_name AS root_category_name
    FROM category_tree
    WHERE parent_category_id IS NULL
),

clicks_prices AS (
    SELECT
        c.job_id,
        j.category_id,
        COALESCE(pl.click_price, j.click_price) AS applied_click_price
    FROM click AS c
    JOIN job AS j ON j.id = c.job_id
    LEFT JOIN LATERAL (
        SELECT click_price
        FROM price_log AS pl
        WHERE pl.job_id = c.job_id AND pl.date_time <= c.date_time
        ORDER BY pl.date_time DESC
        LIMIT 1
    ) AS pl
      ON TRUE
),

revenue_per_category AS (
    SELECT category_id, SUM(applied_click_price) AS revenue
    FROM clicks_prices
    GROUP BY category_id
)

SELECT 
    f.root_category_id,
    f.root_category_name,
    SUM(r.revenue) AS total_revenue
FROM revenue_per_category AS r
JOIN final_categories AS f ON r.category_id = f.category_id
GROUP BY f.root_category_id, f.root_category_name;




