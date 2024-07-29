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
WITH unique_orders_with_delivery_times AS (
SELECT
    order_purchase_timestamp as purchase_time,
    strftime("%Y", order_purchase_timestamp) AS year_number,
    strftime('%m', order_purchase_timestamp) AS month_number,
    JULIANDAY(order_delivered_customer_date) - JULIANDAY(order_purchase_timestamp) AS real_time,
    JULIANDAY(order_estimated_delivery_date) - JULIANDAY(order_purchase_timestamp) AS estimated_time
FROM
    olist_orders
WHERE    
    order_status IS 'delivered'
    AND order_purchase_timestamp IS NOT NULL
    AND order_delivered_customer_date IS NOT NULL
    AND order_estimated_delivery_date IS NOT NULL
GROUP BY
    order_id)
SELECT
    month_number as month_no,
    substr('JanFebMarAprMayJunJulAugSepOctNovDec', (CAST(strftime('%m', purchase_time) AS INTEGER) - 1) * 3 + 1, 3) AS month,
    AVG(CASE 
        WHEN year_number = "2016"  
        THEN real_time
        ELSE NULL
     END) AS Year2016_real_time,
     AVG(CASE 
        WHEN year_number = "2017"  
        THEN real_time
        ELSE NULL
     END) AS Year2017_real_time,
     AVG(CASE 
        WHEN year_number = "2018"  
        THEN real_time
        ELSE NULL
     END) AS Year2018_real_time,
         AVG(CASE 
        WHEN year_number = "2016"  
        THEN estimated_time
        ELSE NULL
     END) AS Year2016_estimated_time,
     AVG(CASE 
        WHEN year_number = "2017"  
        THEN estimated_time
        ELSE NULL
     END) AS Year2017_estimated_time,
     AVG(CASE 
        WHEN year_number = "2018"  
        THEN estimated_time
        ELSE NULL
     END) AS Year2018_estimated_time
    FROM
unique_orders_with_delivery_times
GROUP BY 
    month_number;