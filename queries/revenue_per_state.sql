-- This query will return a table with two columns; customer_state, and 
-- Revenue. The first one will have the letters that identify the top 10 states 
-- with most revenue and the second one the total revenue of each.
-- HINT: All orders should have a delivered status and the actual delivery date 
-- should be not null. 
WITH orders_with_revenue AS (
    SELECT 
        customers.customer_state, 
        payments.payment_value
    FROM 
        olist_orders orders 
    INNER JOIN olist_customers customers ON orders.customer_id = customers.customer_id
    INNER JOIN olist_order_payments payments ON orders.order_id = payments.order_id
    WHERE 
        orders.order_status IS 'delivered' AND orders.order_delivered_customer_date IS NOT NULL
)

SELECT 
    customer_state, 
    SUM(payment_value) AS Revenue
FROM 
    orders_with_revenue
GROUP BY 
    customer_state
ORDER BY 
    Revenue DESC
LIMIT 10;