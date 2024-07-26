-- This query will return a table with the top 10 least revenue categories 
-- in English, the number of orders and their total revenue. The first column 
-- will be Category, that will contain the top 10 least revenue categories; the 
-- second one will be Num_order, with the total amount of orders of each 
-- category; and the last one will be Revenue, with the total revenue of each 
-- category.
-- HINT: All orders should have a delivered status and the Category and actual 
-- delivery date should be not null.
WITH categories_with_orders AS (
    SELECT
        translation.product_category_name_english as Category,
        orders.order_id,
        payments.payment_value
    FROM
        olist_products products
    INNER JOIN olist_order_items items 
        ON products.product_id = items.product_id
    INNER JOIN product_category_name_translation translation 
        ON products.product_category_name = translation.product_category_name
    INNER JOIN olist_orders orders 
        ON items.order_id = orders.order_id
    INNER JOIN olist_order_payments payments 
        ON payments.order_id = orders.order_id
    WHERE
        orders.order_status IS 'delivered'
        AND orders.order_delivered_customer_date IS NOT NULL
        AND products.product_category_name IS NOT NULL
)
SELECT
    Category,
    COUNT(DISTINCT (order_id)) AS Num_order,
    SUM(payment_value) AS Revenue
FROM
    categories_with_orders
GROUP BY
    Category
ORDER BY
    SUM(payment_value) ASC
LIMIT 10;
