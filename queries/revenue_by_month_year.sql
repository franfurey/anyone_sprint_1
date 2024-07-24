-- This query will return a table with the revenue by month and year. It
-- will have different columns: month_no, with the month numbers going from 01
-- to 12; month, with the 3 first letters of each month (e.g. Jan, Feb);
-- Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist);
-- Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and
-- Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).

SELECT 
    strftime('%m', order_purchase_timestamp) AS month_no,
    CASE 
        WHEN strftime('%m', order_purchase_timestamp) = '01' THEN 'Jan'
        WHEN strftime('%m', order_purchase_timestamp) = '02' THEN 'Feb'
        WHEN strftime('%m', order_purchase_timestamp) = '03' THEN 'Mar'
        WHEN strftime('%m', order_purchase_timestamp) = '04' THEN 'Apr'
        WHEN strftime('%m', order_purchase_timestamp) = '05' THEN 'May'
        WHEN strftime('%m', order_purchase_timestamp) = '06' THEN 'Jun'
        WHEN strftime('%m', order_purchase_timestamp) = '07' THEN 'Jul'
        WHEN strftime('%m', order_purchase_timestamp) = '08' THEN 'Aug'
        WHEN strftime('%m', order_purchase_timestamp) = '09' THEN 'Sep'
        WHEN strftime('%m', order_purchase_timestamp) = '10' THEN 'Oct'
        WHEN strftime('%m', order_purchase_timestamp) = '11' THEN 'Nov'
        WHEN strftime('%m', order_purchase_timestamp) = '12' THEN 'Dec'
    END AS month,
    SUM(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2016' THEN payment_value ELSE 0 END) AS Year2016,
    SUM(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2017' THEN payment_value ELSE 0 END) AS Year2017,
    SUM(CASE WHEN strftime('%Y', order_purchase_timestamp) = '2018' THEN payment_value ELSE 0 END) AS Year2018
FROM 
    olist_orders 
JOIN 
...
    month_no

-- WITH revenue_data AS (
--     SELECT
--         strftime('%Y', o.order_purchase_timestamp) AS year,
--         strftime('%m', o.order_purchase_timestamp) AS month,
--         SUM(oi.price) AS monthly_revenue
--     FROM
--         olist_orders o
--     JOIN
--         olist_order_items oi ON o.order_id = oi.order_id
--     WHERE
--         o.order_status != 'canceled'
--     GROUP BY
--         year, month
-- ),
-- formatted_revenue AS (
--     SELECT
--         month,
--         CASE month
--             WHEN '01' THEN 'Jan'
--             WHEN '02' THEN 'Feb'
--             WHEN '03' THEN 'Mar'
--             WHEN '04' THEN 'Apr'
--             WHEN '05' THEN 'May'
--             WHEN '06' THEN 'Jun'
--             WHEN '07' THEN 'Jul'
--             WHEN '08' THEN 'Aug'
--             WHEN '09' THEN 'Sep'
--             WHEN '10' THEN 'Oct'
--             WHEN '11' THEN 'Nov'
--             WHEN '12' THEN 'Dec'
--         END AS month_name,
--         SUM(CASE WHEN year = '2016' THEN monthly_revenue ELSE 0 END) AS Year2016,
--         SUM(CASE WHEN year = '2017' THEN monthly_revenue ELSE 0 END) AS Year2017,
--         SUM(CASE WHEN year = '2018' THEN monthly_revenue ELSE 0 END) AS Year2018
--     FROM
--         revenue_data
--     GROUP BY
--         month
--     ORDER BY
--         month
-- )

-- SELECT
--     month AS month_no,
--     month_name AS month,
--     COALESCE(Year2016, 0.00) AS Year2016,
--     COALESCE(Year2017, 0.00) AS Year2017,
--     COALESCE(Year2018, 0.00) AS Year2018
-- FROM
--     formatted_revenue;
