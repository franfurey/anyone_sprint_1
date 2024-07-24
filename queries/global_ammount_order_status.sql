-- This query will return a table with two columns; order_status, and
-- Amount. The first one will have the different order status classes and the
-- second one the total amount of each.
SELECT 
    order_status AS order_status,
    COUNT(order_id) AS Ammount
FROM 
    olist_orders
GROUP BY 
    order_status
ORDER BY 
    order_status ASC;
