SELECT *
FROM [olist].[dbo].[fato_order_items]

-- query para criacao mapas
create view vw_bi_cities as 
SELECT  a.geolocation_city  as city 
       ,a.geolocation_state as state 
       ,b.price as price
       ,cast(a.geolocation_lat as float) as lat
       ,cast(a.geolocation_lng as float) as lng
FROM dbo.dim_geolocation a inner join dbo.fato_order_items b on a.geolocation_pk = b.geolocation_pk

-- view de produtos
create or alter view vw_bi_product as
SELECT distinct b.product_category_name   as produtos
      ,cast(sum(a.price)as numeric(15,2)) as max_price
      ,cast(min(a.price)as numeric(15,2)) as min_price
      ,count(*) total 
FROM olist.dbo.fato_order_items a inner join olist.dbo.dim_products b on a.product_id = b.product_id
group by b.product_category_name


select *
--  produtos,
--  sum(max_price) max_price,
--  sum(min_price) min_price,
--  total
from vw_bi_product
group by produtos,total
