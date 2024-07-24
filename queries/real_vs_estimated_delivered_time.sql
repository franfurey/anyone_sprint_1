-- This query will return a table with the differences between the real 
-- and estimated delivery times by month and year. It will have different 
-- columns: month_no, with the month numbers going from 01 to 12; month, with 
-- the 3 first letters of each month (e.g. Jan, Feb); Year2016_real_time, with 
-- the average delivery time per month of 2016 (NaN if it doesn't exist); 
-- Year2017_real_time, with the average delivery time per month of 2017 (NaN if 
-- it doesn't exist); Year2018_real_time, with the average delivery time per 
-- month of 2018 (NaN if it doesn't exist); Year2016_estimated_time, with the 
-- average estimated delivery time per month of 2016 (NaN if it doesn't exist); 
-- Year2017_estimated_time, with the average estimated delivery time per month 
-- of 2017 (NaN if it doesn't exist) and Year2018_estimated_time, with the 
-- average estimated delivery time per month of 2018 (NaN if it doesn't exist).
-- HINTS
-- 1. You can use the julianday function to convert a date to a number.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Take distinct order_id.

SELECT 
    strftime('%m', order_delivered_customer_date) as month_no,
    CASE
        WHEN strftime('%m', order_delivered_customer_date) = '01' THEN 'Jan'
        WHEN strftime('%m', order_delivered_customer_date) = '02' THEN 'Feb'
        WHEN strftime('%m', order_delivered_customer_date) = '03' THEN 'Mar'
        WHEN strftime('%m', order_delivered_customer_date) = '04' THEN 'Apr'
        WHEN strftime('%m', order_delivered_customer_date) = '05' THEN 'May'
        WHEN strftime('%m', order_delivered_customer_date) = '06' THEN 'Jun'
        WHEN strftime('%m', order_delivered_customer_date) = '07' THEN 'Jul'
        WHEN strftime('%m', order_delivered_customer_date) = '08' THEN 'Aug'
        WHEN strftime('%m', order_delivered_customer_date) = '09' THEN 'Sep'
        WHEN strftime('%m', order_delivered_customer_date) = '10' THEN 'Oct'
        WHEN strftime('%m', order_delivered_customer_date) = '11' THEN 'Nov'
        WHEN strftime('%m', order_delivered_customer_date) = '12' THEN 'Dec'
    END as month,
    AVG(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2016' THEN julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp) ELSE NULL END) as Year2016_real_time,
    AVG(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2017' THEN julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp) ELSE NULL END) as Year2017_real_time,
    AVG(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2018' THEN julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp) ELSE NULL END) as Year2018_real_time,
    AVG(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2016' THEN julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp) ELSE NULL END) as Year2016_estimated_time,
    AVG(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2017' THEN julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp) ELSE NULL END) as Year2017_estimated_time,
    AVG(CASE WHEN strftime('%Y', order_delivered_customer_date) = '2018' THEN julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp) ELSE NULL END) as Year2018_estimated_time
FROM 
    olist_orders
WHERE 
    order_status = 'delivered' AND 
    order_delivered_customer_date IS NOT NULL
GROUP BY 
    month_no
ORDER BY 
    month_no

-- WITH delivery_times AS (
--     SELECT 
--         order_id,
--         strftime('%m', order_delivered_customer_date) AS month_no,
--         strftime('%Y', order_delivered_customer_date) AS year,
--         julianday(order_delivered_customer_date) - julianday(order_estimated_delivery_date) AS delivery_diff,
--         julianday(order_delivered_customer_date) - julianday(order_purchase_timestamp) AS real_time,
--         julianday(order_estimated_delivery_date) - julianday(order_purchase_timestamp) AS estimated_time
--     FROM 
--         olist_orders
--     WHERE 
--         order_status = 'delivered' 
--         AND order_delivered_customer_date IS NOT NULL
-- )

-- SELECT 
--     month_no,
--     CASE month_no
--         WHEN '01' THEN 'Jan'
--         WHEN '02' THEN 'Feb'
--         WHEN '03' THEN 'Mar'
--         WHEN '04' THEN 'Apr'
--         WHEN '05' THEN 'May'
--         WHEN '06' THEN 'Jun'
--         WHEN '07' THEN 'Jul'
--         WHEN '08' THEN 'Aug'
--         WHEN '09' THEN 'Sep'
--         WHEN '10' THEN 'Oct'
--         WHEN '11' THEN 'Nov'
--         WHEN '12' THEN 'Dec'
--     END AS month,
--     AVG(CASE WHEN year = '2016' THEN real_time ELSE NULL END) AS Year2016_real_time,
--     AVG(CASE WHEN year = '2017' THEN real_time ELSE NULL END) AS Year2017_real_time,
--     AVG(CASE WHEN year = '2018' THEN real_time ELSE NULL END) AS Year2018_real_time,
--     AVG(CASE WHEN year = '2016' THEN estimated_time ELSE NULL END) AS Year2016_estimated_time,
--     AVG(CASE WHEN year = '2017' THEN estimated_time ELSE NULL END) AS Year2017_estimated_time,
--     AVG(CASE WHEN year = '2018' THEN estimated_time ELSE NULL END) AS Year2018_estimated_time
-- FROM 
--     delivery_times
-- GROUP BY 
--     month_no
-- ORDER BY 
--     month_no;
