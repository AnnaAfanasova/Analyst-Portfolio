-- 1. Чи є в запропонованих даних дублі? Як це визначити за допомогою SQL запиту? 

-- Відповідь:
-- Так, 138 ідентичних рядків у таблиці installs, 3 рядки — у таблиці cards. 
-- Також таблиця installs містить 651 install_id, які належать більше ніж одному user_id.


-- 1.1. Перевірка ідентичних рядків

-- Логіка рішення:
-- Для кожної таблиці окремо групую рядки за всіма колонками, щоб знайти повністю ідентичні.
-- Виводжу лише ті комбінації, що трапляються більше одного разу через СOUNT > 1.
-- Об'єдную результати для всіх таблиць в одному запиті через UNION ALL.

SELECT 'installs' AS table_name, COUNT(*) AS duplicate_count
FROM (
    SELECT install_id, user_id, installed_date, traffic_source, os, COUNT(*) AS cnt
    FROM installs
    GROUP BY install_id, user_id, installed_date, traffic_source, os
    HAVING COUNT(*) > 1
) AS sub_installs

UNION ALL

SELECT 'cards' AS table_name, COUNT(*) AS duplicate_count
FROM (
    SELECT user_id, activated_card_date, credit_limit_amount, COUNT(*) AS cnt
    FROM cards
    GROUP BY user_id, activated_card_date, credit_limit_amount
    HAVING COUNT(*) > 1
) AS sub_cards

UNION ALL

SELECT 'costs' AS table_name, COUNT(*) AS duplicate_count
FROM (
    SELECT date, traffic_source, os, cost_amount, COUNT(*) AS cnt
    FROM costs
    GROUP BY date, traffic_source, os, cost_amount
    HAVING COUNT(*) > 1
) AS sub_costs;

-- 1.2. Перевірка унікальності install_id:

-- Групую за install_id, рахую кількість неунікальних user_id. 

SELECT install_id, COUNT(DISTINCT user_id) AS user_count, STRING_AGG(user_id, ', ' ORDER BY user_id) AS user_ids
FROM installs
GROUP BY install_id
HAVING COUNT(DISTINCT user_id) > 1
ORDER BY user_count DESC;

-- 1.3. Створення очищеної таблиці installs_clean:

-- Видаляю повні дублікати та install_id, які зустрічаються у кількох user_id.
-- У реальних умовах такі аномалії варто дослідити окремо — це може бути технічна помилка або особливість бізнес-логіки.
-- У межах тестового завдання очищаю дані для точного розрахунку метрик.
-- У подальших запитах працюю лише з installs_clean.


CREATE VIEW installs_clean AS
SELECT *
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY install_id, user_id, installed_date, traffic_source, os ORDER BY installed_date) AS rn
    FROM installs
    WHERE install_id NOT IN (SELECT install_id FROM installs GROUP BY install_id HAVING COUNT(DISTINCT user_id) > 1)
) AS sub
WHERE rn = 1;


-- 2. Чи є в запропонованих даних пропуски в днях? Як це визначити за допомогою SQL запиту?

--Відповідь: 
--Так, у таблиці cards пропущено 2025-02-16, у таблиці installs - 2025-02-08 

-- Логіка рішення:
-- Будую повний список дат для кожної таблиці у діапазоні від MIN() до MAX() через generate_series().
-- Визначаю фактичні дати з таблиць.
-- Знаходжу дні, яких не вистачає за допомогою LEFT JOIN.
-- Об'єдную всі результати в один запит.

WITH 
installs_dates AS (
  SELECT generate_series(
           (SELECT MIN(installed_date) FROM installs_clean),
           (SELECT MAX(installed_date) FROM installs_clean),
           INTERVAL '1 day'
         )::date AS dt
),
installs_existing AS (
  SELECT DISTINCT installed_date::date AS dt FROM installs_clean
),
installs_missing AS (
  SELECT 'installs' AS table_name, d.dt AS missing_date
  FROM installs_dates d
  LEFT JOIN installs_existing e ON d.dt = e.dt
  WHERE e.dt IS NULL
),

cards_dates AS (
  SELECT generate_series(
           (SELECT MIN(activated_card_date) FROM cards),
           (SELECT MAX(activated_card_date) FROM cards),
           INTERVAL '1 day'
         )::date AS dt
),
cards_existing AS (
  SELECT DISTINCT activated_card_date::date AS dt FROM cards
),
cards_missing AS (
  SELECT 'cards' AS table_name, d.dt AS missing_date
  FROM cards_dates d
  LEFT JOIN cards_existing e ON d.dt = e.dt
  WHERE e.dt IS NULL
),

costs_dates AS (
  SELECT generate_series(
           (SELECT MIN(date) FROM costs),
           (SELECT MAX(date) FROM costs),
           INTERVAL '1 day'
         )::date AS dt
),
costs_existing AS (
  SELECT DISTINCT date::date AS dt FROM costs
),
costs_missing AS (
  SELECT 'costs' AS table_name, d.dt AS missing_date
  FROM costs_dates d
  LEFT JOIN costs_existing e ON d.dt = e.dt
  WHERE e.dt IS NULL
)

SELECT * FROM installs_missing
UNION ALL
SELECT * FROM cards_missing
UNION ALL
SELECT * FROM costs_missing
ORDER BY table_name, missing_date;

-- 3. Порахувати і вивести долю повторних інсталів на користувача в динаміці по дням. А також окремо по сорсам трафіку.

--3.1 Повторні інстали по днях і трафік-джерелах

-- Логіка рішення:
-- Присвоюю кожному user_id номер інсталу (ROW_NUMBER), щоб визначити, який інсталл був першим.
-- Встановлюю прапорець is_repeat = 1 для всіх інсталів після першого.
-- Групую дані по даті та трафік-джерелу.


WITH installs_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS row_num
  FROM installs_clean
),
installs_flagged AS (
  SELECT installed_date, traffic_source, 
    CASE 
      WHEN row_num = 1 THEN 0 
      ELSE 1 
    END AS is_repeat
  FROM installs_ranked
)
SELECT installed_date, traffic_source, COUNT(*) AS total_installs, SUM(is_repeat) AS reinstall_count,
  ROUND(100.0 * SUM(is_repeat)::decimal / COUNT(*), 1) AS reinstall_rate_percent
FROM installs_flagged
GROUP BY installed_date, traffic_source
ORDER BY installed_date, traffic_source;


--3.2. Загальна к-сть повторних інсталів по джерелах трафіку

-- Логіка рішення:
-- Присвоюю кожному user_id номер інсталу через ROW_NUMBER.
-- Позначаю повторні (is_repeat = 1) інстали.
-- Групую по traffic_source.

WITH installs_ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS row_num
  FROM installs_clean
),
installs_flagged AS (
  SELECT traffic_source, 
    CASE 
       WHEN row_num = 1 THEN 0 
       ELSE 1 
    END AS is_repeat
  FROM installs_ranked
)
SELECT traffic_source, COUNT(*) AS total_installs, SUM(is_repeat) AS reinstall_count,
  ROUND(100.0 * SUM(is_repeat)::decimal / COUNT(*), 1) AS reinstall_rate_percent
FROM installs_flagged
GROUP BY traffic_source
ORDER BY reinstall_rate_percent DESC;


-- 4. Порахувати конверсію з унікальних перших інсталів в активовану картку по дням окремо, по операційним системам і загалом.

-- 4.1 Конверсія перших інсталів по дням

-- Логіка рішення:
--Визначаю перший інсталл для кожного user_id за допомогою ROW_NUMBER().
--Відкидаю повторні інстали.
--Роблю join таблиць cards по user_id, щоб визначити, хто активував картку.
--Враховую тільки тих, хто активував картку не пізніше ніж через 7 днів після інсталу.
--Групую по installed_date, рахую загальну кількість інсталів та кількість активацій.
--Обчислюю конверсію у відсотках.

WITH first_installs AS (
  SELECT user_id, installed_date
  FROM (SELECT user_id, installed_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS row_num FROM installs_clean) AS ranked
  WHERE row_num = 1
)
SELECT f.installed_date, COUNT(*) AS total_installs, COUNT(c.user_id) AS activated_within_7d, ROUND(100.0 * COUNT(c.user_id)::decimal / COUNT(*), 1) AS conversion_rate_percent
FROM first_installs f
LEFT JOIN cards c ON f.user_id = c.user_id AND c.activated_card_date <= f.installed_date + INTERVAL '7 days'
GROUP BY f.installed_date
ORDER BY f.installed_date;

-- 4.2 Конверсія по дням і ОС

-- Логіка рішення:
-- Визначаю перший інсталл користувача через ROW_NUMBER().
-- Додаю поле os, фільтрую NULL-значення в колонці os.
-- Джойню з таблицею cards по user_id з обмеженням у 7 днів до активації.
-- Групую по даті та операційній системі.
-- Рахую конверсію окремо для кожної ОС на кожен день.

WITH first_installs AS (
  SELECT user_id, installed_date, os
  FROM (SELECT user_id, installed_date, os, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS row_num FROM installs_clean) AS ranked
  WHERE row_num = 1
)
SELECT f.installed_date, f.os, COUNT(*) AS total_installs, COUNT(c.user_id) AS activated_within_7d, ROUND(100.0 * COUNT(c.user_id)::decimal / COUNT(*), 1) AS conversion_rate_percent
FROM first_installs AS f
LEFT JOIN cards c ON f.user_id = c.user_id AND c.activated_card_date <= f.installed_date + INTERVAL '7 days'
WHERE f.os IS NOT NULL
GROUP BY f.installed_date, f.os
ORDER BY f.installed_date, f.os;


-- 4.3. Конверсія окремо по операційним системам

-- Логіка рішення:
-- Визначаю перший інсталл кожного користувача через ROW_NUMBER().
-- Відкидаю користувачів з NULL у полі os.
-- Джойню з таблицею cards по user_id, враховуючи тільки активації в межах 7 днів після інсталу.
-- Групую по os, рахую кількість інсталів та активацій, обчислюю конверсію.

WITH first_installs AS (
  SELECT user_id, os, installed_date
  FROM (SELECT user_id, os, installed_date,ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS row_num FROM installs_clean) AS ranked
  WHERE row_num = 1 AND os IS NOT NULL
)
SELECT f.os, COUNT(*) AS total_installs, COUNT(c.user_id) AS activated_within_7d, ROUND(100.0 * COUNT(c.user_id)::decimal / COUNT(*), 1) AS conversion_rate_percent
FROM first_installs AS f
LEFT JOIN cards c ON f.user_id = c.user_id AND c.activated_card_date <= f.installed_date + INTERVAL '7 days'
GROUP BY f.os
ORDER BY conversion_rate_percent DESC;

-- 5. Яка конверсія в активацію картки в перші 3 дні після інсталювання додатку?

-- Логіка рішення:
--Знаходжу перший інсталл кожного користувача через ROW_NUMBER().
--Роблю JOIN з таблицею cards по user_id.
-- Враховую тільки ті активації, які відбулись протягом 3 днів від дати інсталу.
-- Обчислюю конверсію: converted_users / total_first_installs.

WITH first_installs AS (
  SELECT user_id, installed_date
  FROM (
    SELECT user_id, installed_date,
           ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS row_num
    FROM installs_clean
  ) AS ranked
  WHERE row_num = 1
)
SELECT 
  COUNT(*) AS total_first_installs,
  COUNT(c.user_id) AS activated_within_3d,
  ROUND(100.0 * COUNT(c.user_id)::decimal / COUNT(*), 1) AS conversion_rate_percent
FROM first_installs AS f
LEFT JOIN cards c 
  ON f.user_id = c.user_id
  AND c.activated_card_date <= f.installed_date + INTERVAL '3 days';

-- 6. Порахувати вартість залучення однієї картки (CAC) по когортам. Когортою вважаємо дату першого інсталу користувача. Когорта якого дня привела найдешевші картки, а якого найдорожчі? 

--Відповідь: 
--Когорта 2025-02-03 привела найдешевші картки, когорта 2025-02-19 - найдорожчі.


-- Створюю cards_clean — очищену версію таблиці cards без дублів.
-- Дублі визначаю як повне співпадіння user_id, activated_card_date і credit_limit_amount.

CREATE VIEW cards_clean AS
SELECT DISTINCT user_id, activated_card_date, credit_limit_amount
FROM cards;

-- Логіка рішення:
-- CTE first_installs — вибираю перший інстал кожного користувача як cohort_date.
-- CTE costs_agg — сума витрат по датах (когортам).
-- CTE activations — рахую активацій карток по когортах у межах 7 днів.
-- Фінальний SELECT — з’єдную витрати і активацій, обчислюю CAC = total_cost/activated_users, ROUND до двох знаків, NULL якщо activated_users = 0.

WITH first_installs AS (
  SELECT user_id, installed_date AS cohort_date
  FROM (
    SELECT user_id, installed_date, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS rn
    FROM installs_clean
  ) t
  WHERE rn = 1
),

costs_agg AS (
  SELECT date AS cohort_date, SUM(cost_amount) AS total_cost
  FROM costs
  GROUP BY date
),

activations AS (
  SELECT f.cohort_date, COUNT(*) AS activated_users
  FROM first_installs AS f
  JOIN cards_clean AS c ON c.user_id = f.user_id AND c.activated_card_date <= f.cohort_date + INTERVAL '7 days'
  GROUP BY f.cohort_date
)

SELECT co.cohort_date, co.total_cost, COALESCE(a.activated_users, 0) AS activated_users,
  CASE
    WHEN COALESCE(a.activated_users, 0) = 0 THEN NULL
    ELSE ROUND(co.total_cost::numeric / a.activated_users, 2)
  END AS CAC
FROM costs_agg co
LEFT JOIN activations AS a USING (cohort_date)
ORDER BY CAC;


-- 7. Скільки карток було фактично активовано за останні 10 днів від сьогоднішнього дня? Вважаємо, що сьогодні 20 лютого 2025 року

--Логіка:
-- Створюю CTE current_day з фіксованою датою '2025-02-20'.
-- Фільтрую записи, де activated_card_date знаходиться в інтервалі current_day - 10 днів.
-- Рахую кількість таких карток.

WITH current_day AS (
  SELECT DATE '2025-02-20' AS today
)
SELECT COUNT(*) AS activated_cards_last_10_days
FROM cards_clean, current_day
WHERE activated_card_date BETWEEN today - INTERVAL '10 days' AND today;


-- 8. Який апрувал рейт кредитного ліміту? А який для ліміту більше 60 баксів?

-- Логіка рішення:
-- Створюю CTE, де для кожного користувача перевіряю:
-- Чи його заявка була схвалена (ліміт > 0)
-- Чи його ліміт перевищує 60

WITH card_flags AS (
  SELECT 
    user_id,
    credit_limit_amount,
    CASE WHEN credit_limit_amount > 0 THEN 1 ELSE 0 END AS is_approved,
    CASE WHEN credit_limit_amount > 60 THEN 1 ELSE 0 END AS is_approved_above_60
  FROM cards_clean
)

-- 2. Рахую загальну кількість користувачів, загальний рейт, рейт > 60$

SELECT
  COUNT(*) AS total_users,
  SUM(is_approved) AS approved_users,
  ROUND(100.0 * SUM(is_approved)::numeric / COUNT(*), 1) AS approval_rate_percent,
  SUM(is_approved_above_60) AS approved_above_60$_users,
  ROUND(100.0 * SUM(is_approved_above_60)::numeric / COUNT(*), 1) AS approval_rate_above_60$_percent
FROM card_flags;


-- 9. Вважаємо що з кожного кредитного ліміту ми отримуємо 1% заробітку. Необхідно порахуйте ROI по трафік сорсам і в тоталі.

-- Логіка рішення:
-- Створюю CTE first_installs, де для кожного користувача визначаю перший інстал і джерело трафіку.
-- Агрегую витрати у CTE costs_by_source по кожному traffic_source за весь період.
-- Агрегую дохід у CTE revenue_by_source як 1% від credit_limit для активацій у межах 7 днів з дня інсталу.
-- У CTE roi_by_source виконую FULL OUTER JOIN витрат і доходів, щоб включити навіть ті джерела (наприклад organic) з нульовими витратами чи доходами.
-- Розраховую ROI = (total_revenue – total_cost) / total_cost, округлюю до двох знаків; якщо total_cost = 0 — роблю NULL.
-- У CTE total_row збираю підсумок по всіх джерелах: сумую total_cost і total_revenue, раху­ю загальний ROI.
-- Об’єдную рядки roi_by_source та total_row у підзапиті, виводжу їх з ORDER BY так, щоб 'total' був останнім.


WITH first_installs AS (
    SELECT user_id, installed_date, traffic_source
    FROM (
      SELECT user_id, installed_date, traffic_source, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY installed_date) AS rn
      FROM installs_clean
    ) t
    WHERE rn = 1
  ),

  costs_by_source AS (
    SELECT traffic_source, SUM(cost_amount) AS total_cost
    FROM costs
    GROUP BY traffic_source
  ),

  revenue_by_source AS (
    SELECT f.traffic_source, SUM(c.credit_limit_amount) * 0.01 AS total_revenue
    FROM first_installs f
    JOIN cards_clean c ON c.user_id = f.user_id AND c.activated_card_date BETWEEN f.installed_date AND f.installed_date + INTERVAL '7 days'
    GROUP BY f.traffic_source
  ),

  roi_by_source AS (
    SELECT
      COALESCE(s.traffic_source, r.traffic_source) AS traffic_source,
      COALESCE(s.total_cost,   0) AS total_cost,
      COALESCE(r.total_revenue,0) AS total_revenue,
      CASE
        WHEN COALESCE(s.total_cost, 0) = 0 THEN NULL
        ELSE ROUND((COALESCE(r.total_revenue, 0) - COALESCE(s.total_cost, 0)) / COALESCE(s.total_cost, 0), 2)
      END AS roi
    FROM costs_by_source s
    FULL OUTER JOIN revenue_by_source r ON s.traffic_source = r.traffic_source
  ),

  total_row AS (
    SELECT 'total' AS traffic_source, SUM(total_cost) AS total_cost, SUM(total_revenue) AS total_revenue,
      CASE
        WHEN SUM(total_cost) = 0 THEN NULL
        ELSE ROUND((SUM(total_revenue) - SUM(total_cost)) / SUM(total_cost), 2)
      END AS roi
    FROM roi_by_source
  )

SELECT *
FROM (
  SELECT traffic_source, total_cost, total_revenue, roi
  FROM roi_by_source
  UNION ALL
  SELECT traffic_source, total_cost, total_revenue, roi
  FROM total_row
) AS combined
ORDER BY CASE WHEN traffic_source = 'total' THEN 1 ELSE 0 END, traffic_source;
