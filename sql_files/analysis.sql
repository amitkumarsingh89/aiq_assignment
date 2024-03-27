-- Based on Dataset I have done below Analysis. Please see the email attachment
-- 1) Lifetime Revenue
-- 2) Lifetime Orders
-- 3) Lifetime Quantity
-- 4) Lifetime AOV
-- 5) Lifetime AOQ
-- 6) Top Products By Revenue
-- 7) Top Customers By Revenue
-- 8) Monthly Order Distribution
-- 9) Top Cities By Orders
-- 10) Order Based On Temperature
-- 11) Order Based On Weather Condition

------ Customer sales amount, orders, quantity, aov & aoq (we can also achieve top products by limiting the number) -------------
select customer_id, name, email, count(distinct order_id) as orders,
       sum(quantity) as qty, sum(price) as rev, round(sum(price)::numeric/count(distinct order_id),1) as aov,
       round(sum(quantity)::numeric/count(distinct order_id),1) as aoq
       from  aiq_data.sales_order s
    join aiq_data.customer_details c on c.id = customer_id group by 1,2,3 order by 6 desc;

-------- Sales By Products (we can also achieve top products by limiting the number)  -------------
select product_id, count(distinct order_id) as orders,
       sum(quantity) as qty, sum(price) as rev, round(sum(price)::numeric/count(distinct order_id),1) as aov,
       round(sum(quantity)::numeric/count(distinct order_id),1) as aoq
       from aiq_data.sales_order group by 1 order by 4 desc;

--------- Raw Data  -------------
with ord as
         (
             select s.*,
                    c.name,
                    c.username,
                    c.email,
                    c.city,
                    c.zipcode,
                    c.lat,
                    c.lon,
                    w.weather_type,
                    w.description,
                    w.base,
                    w.temp,
                    w.pressure,
                    w.humidity,
                    w.visibility,
                    w.wind_speed,
                    w.wind_gust,
                    w.wind_deg,
                    w.cloud_all,
                    case
                        when temp < 240 then 'Temp < 240'
                        when temp > 240 and temp <= 260 then 'Temp - 240-260'
                        when temp > 260 and temp <= 280 then 'Temp - 260-280'
                        when temp > 280 and temp <= 300 then 'Temp - 280-300'
                        when temp > 300 then 'Temp > 300'
                        else null end as temp_agg
             from aiq_data.sales_order s
                      left join aiq_data.customer_details c on c.id = customer_id
                      left join aiq_data.weather_details w on w.lat = c.lat and w.lon = c.lon -- Date should also be required here but date doesn't match and why I ignored it.
         )
select * from ord;






