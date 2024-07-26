-- TODO: This query will return a table with the revenue by month and year. It
-- will have different columns: month_no, with the month numbers going from 01
-- to 12; month, with the 3 first letters of each month (e.g., Jan, Feb);
-- Year2016, with the revenue per month of 2016 (0.00 if it doesn't exist);
-- Year2017, with the revenue per month of 2017 (0.00 if it doesn't exist) and
-- Year2018, with the revenue per month of 2018 (0.00 if it doesn't exist).
select month_no,
       CASE month_no
           WHEN '01' THEN 'Jan'
           WHEN '02' THEN 'Feb'
           WHEN '03' THEN 'Mar'
           WHEN '04' THEN 'Apr'
           WHEN '05' THEN 'May'
           WHEN '06' THEN 'Jun'
           WHEN '07' THEN 'Jul'
           WHEN '08' THEN 'Aug'
           WHEN '09' THEN 'Sep'
           WHEN '10' THEN 'Oct'
           WHEN '11' THEN 'Nov'
           WHEN '12' THEN 'Dec'
       END AS month,  -- 'Jan'
       coalesce(sum(
           case year_no
               when '2016' then Revenue
               else 0
           end
       ),0.00) as 'Year2016',
       coalesce(sum(
           case year_no
               when '2017' then Revenue
               else 0
           end
       ),0.00) as 'Year2017',
       coalesce(sum(
           case year_no
               when '2018' then Revenue
               else 0
           end
       ),0.00) as 'Year2018'
from (
    SELECT strftime('%m', order_delivered_customer_date) As 'month_no',
           strftime('%Y', order_delivered_customer_date) As 'year_no',
           i.payment_value As 'Revenue'
    FROM olist_orders o
    INNER JOIN olist_order_payments i ON o.order_id = i.order_id
    WHERE order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL
    GROUP BY o.customer_id
) as t1
group by month_no
order by month_no;
