# Проектирование хранилища

## Анализ исходных атрибутов

Исходя из описания [датасета](https://www.kaggle.com/datasets/roopacalistus/superstore) и зависимостями между атрибутами, можно поделить все атрибуты исходного датасета на следующие группы:

- атрибуты доставки
    - Ship Mode
- атрибуты клиента
    - Segment
- атрибуты геолокации магазина
    - Country
    - City
    - State
    - Postal Code
    - Region
- атрибуты товаров
    - Category
    - Sub-Category
- факты продаж
    - Sales
    - Quantity
    - Discount
    - Profit

## Проектирование модели хранилища

Атрибуты доставки, клиента, геолокации магазина и товаров являются 
строковыми значениями, которые описывают исходные сущности,
поэтому для каждой из групп следует создать хаб с суррогатным ключом, который можно сформировать используя хеш из атрибутов.

Так как одна строка в датасете является одной продажей (есть несколько записей, у которых все атрибуты одинаковые, а факты разные), то потребуется создать отдельный хаб для продаж.

### Хабы

Каждый хаб будет представлять собой суррогатный ключ и стамп времени создания записи. Так как естественного ключа в датасете нет, не будем добавлять его в модель данных. Суррогатный ключ можно создать

- hub_shipments
    - shipment_id
    - created_at
- hub_clients
    - client_id
    - created_at
- hub_stores
    - store_id
    - created_at
- hub_products
    - product_id
    - created_at
- hub_sales
    - sale_id
    - created_at

### Ссылки

Так как основные все атрибуты завязаны на факт продажи, необходимо связать хаб продаж 
с хабами всех атрибутов. Вариант создание одной ссылки, в которой ключом были бы
сразу пять суррогатных ключей всех хабов плох тем, что при добавлении нового ключа 
в модель данных придется менять таблицу ссылок и обновлять уже существующие связи,
а это идет в разрез с самой концепцией Data Vault. Поэтому для каждой группы атрибутов будет создаваться
отдельная ссылка между хабом продаж и хабом атрибута, а при добавлении новых ключевых
атрибутов - создаваться новая таблица-ссылка

- link_sales_shipments
    - sale_id
    - shipment_id
    - created_at
- link_sales_clients
    - sale_id
    - client_id
    - created_at
- link_sales_stores
    - sale_id
    - store_id
    - created_at
- link_sales_products
    - sale_id
    - product_id
    - created_at

### Сателлиты

В сателлитах будут храниться атрибуты каждого хаба с штампом времени начала действия записи, а в сателлите продаж - факты продаж.

- sat_shipments
    - shipment_id
    - shipment_mode
    - effective_from
- sat_clients
    - client_id
    - client_segment
    - effective_from
- sat_store
    - store_id
    - country
    - city
    - state
    - postal_code
    - region
    - effective_from
- sat_products
    - product_id
    - category
    - subcategory
    - effective_from
- sat_sales
    - sale_id
    - sales
    - quantity
    - discount
    - profit
    - effective_from



