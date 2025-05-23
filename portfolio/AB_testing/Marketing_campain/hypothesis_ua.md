# A/B Тестування: Вплив реклами на конверсію

## Огляд експерименту

Цей експеримент спрямований на пошук відповідей на два ключові питання:

1. **Чи була рекламна кампанія успішною?**
2. **Якщо кампанія була успішною, наскільки саме реклама сприяла цьому успіху?**

Щоб відповісти на ці питання, ми проводимо A/B тестування, у якому:
Експериментальна група бачить рекламні оголошення.
Контрольна група бачить лише соціальну рекламу (PSA) або нічого.

**Мета аналізу** — оцінити ефективність реклами, розрахувати додатковий прибуток, який вона приносить компанії, і перевірити, чи є різниця між групами статистично значущою.

---

## Структура даних:

- **`user id`** – унікальний ідентифікатор користувача.
- **`test group`** – група тестування (`ad` – бачили рекламу, `psa` – бачили лише соціальне оголошення).
- **`converted`** – чи здійснив користувач покупку (`True/False`).
- **`total ads`** – кількість переглянутих рекламних оголошень.
- **`most ads day`** – день тижня, коли користувач побачив найбільше оголошень.
- **`most ads hour`** – година, коли користувач побачив найбільше оголошень.

Цей файл містить формулювання **гіпотези**, метрики успіху та потенційні ризики експерименту.

---

## Картка гіпотези

### **Опис проблеми**: 
Компанія вкладає ресурси в рекламу, але незрозуміло, чи дійсно це сприяє зростанню конверсії. Без аналізу важко оцінити ефективність кампаній і прийняти обґрунтовані рішення щодо їх оптимізації.

### **Мета**
Збільшення конверсії у тестовій групі на **10%** порівняно з контрольною групою.

### **Застосування гіпотези**
Запуск рекламного оголошення для **експериментальної групи (Ad)** та публічного оголошення для **контрольної групи (PSA)**. Ми аналізуємо, чи існує статистично значуща різниця між показниками конверсії в цих двох групах.

### **Очікувані результати**
Якщо гіпотеза підтвердиться, то у тестовій групі (**Ad**) конверсія буде **істотно вищою**, ніж у контрольній групі. Якщо гіпотеза не підтвердиться, різниця у конверсії між групами буде **незначною або відсутньою**.

### **Метрики успіху**
- **Conversion Rate** – частка користувачів, які здійснили покупку після перегляду реклами.
- **P-value** – статистичний тест для оцінки значущості відмінностей.
- **Δ Conversion Rate** – різниця у конверсії між групами.

### **Причина для тестування**
Компанії необхідно оцінити **ефективність реклами** для прийняття рішень щодо рекламного бюджету.

### **Ризики**
- **Викривлення органічної конверсії** – реклама може залучати користувачів, які і так би здійснили покупку.
- **Негативне сприйняття бренду** – через надмірну кількість рекламних показів.
- **Не врахування точки рентабельності** – коли витрати на залучення клієнтів перевищують прибуток від них.

### **Гіпотеза**
**Ми віримо, що рекламна кампанія підвищує коневерсію на 10% у порівнянні з контрольною групою.** 
