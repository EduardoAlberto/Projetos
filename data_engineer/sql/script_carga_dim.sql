/*****************************************
---assunto: script carga das DIM e Fatos
---data   : 23-04-2021 
---Autor  : eduardo alberto
******************************************/

-- olist.dbo.dim_customers
merge olist.dbo.dim_customers T
using olist.dbo.stg_customers S on t.customer_id = s.customer_id
WHEN NOT MATCHED THEN
insert (customer_sgk,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state)
values(s.customer_sgk,
       replace(s.customer_id,'"',''),
       replace(s.customer_unique_id,'"',''),
       convert(int,replace(s.customer_zip_code_prefix,'"','')),
       s.customer_city,
       s.customer_state)
when not matched by source
THEN
delete;

-- olist.dbo.dim_geolocation
merge olist.dbo.dim_geolocation T
using olist.dbo.stg_geolocation S on t.geolocation_zip_code_prefix = convert(int,replace(s.geolocation_zip_code_prefix,'"',''))
when not matched THEN
insert(geolocation_sgk
      ,geolocation_zip_code_prefix
      ,geolocation_lat
      ,geolocation_lng
      ,geolocation_city
      ,geolocation_state)
values(geolocation_sgk
      ,convert(int,replace(s.geolocation_zip_code_prefix,'"',''))
      ,geolocation_lat
      ,geolocation_lng
      ,geolocation_city
      ,replace(replace(geolocation_state,' rio de janeiro, brasil",RJ','RJ'),' bahia, brasil",BA','BA'))
when not matched by source
THEN
delete;

--- dim_order_items
merge olist.dbo.dim_order_items T
using olist.dbo.stg_order_items S on t.order_id = s.order_id
when not matched THEN
insert
(

        order_id
       ,order_item_id
       ,product_id
       ,seller_id
       ,shipping_limit_date
       ,price
       ,freight_value
       ,order_items_sgk
)
values
(

        s.order_id
       ,s.order_item_id
       ,s.product_id
       ,s.seller_id
       ,s.shipping_limit_date
       ,s.price
       ,s.freight_value
       ,s.order_items_sgk

)
when not matched by source
THEN
delete;

select top 10 * from dim_order_items

---olist.dbo.dim_date
insert into olist.dbo.dim_Date
select * from dwproducao.dbo.dimDate 

---olist.dbo.dim_order_reviews
merge olist.dbo.dim_order_payments T
using olist.dbo.stg_order_payments S on t.order_id = replace(s.order_id,'"','')
when not matched THEN
insert ( order_payments_sgk
        ,order_id
        ,payment_sequential
        ,payment_type
        ,payment_installments
        ,payment_value) 
values( order_payments_sgk
       ,replace(order_id,'"','')
       ,convert(int,payment_sequential)
       ,payment_type
       ,convert(int,payment_installments)
       ,convert(float,payment_value))
when not matched by source
THEN
delete;

--sp_help 'stg_order_payments'

-- dim_order_reviews

merge olist.dbo.dim_order_reviews T
using olist.dbo.stg_order_reviews S on t.review_id = s.review_id
when not matched THEN
insert( reviews_sgk
       ,review_id
       ,order_id
       ,review_score
       ,review_comment_title
       ,review_comment_message
       ,review_creation_date
       ,review_answer_timestamp)
values( s.review_sgk
       ,s.review_id
       ,s.order_id
       ,convert(int,review_score)
       ,review_comment_title
       ,review_comment_message
       ,convert(datetime,review_creation_date)
       ,convert(datetime,review_answer_timestamp))
when not matched by source
THEN
delete;

--sp_help 'dim_order_reviews'

-- dim_orders
merge olist.dbo.dim_orders T
using olist.dbo.stg_orders S on t.order_id = s.order_id
when not matched THEN
insert (order_sgk
       ,order_id
       ,customer_id
       ,order_status
       ,order_purchase_timestamp
       ,order_approved_at
       ,order_delivered_carrier_date
       ,order_delivered_customer_date
       ,order_estimated_delivery_date)
VALUES
(       s.order_sgk
       ,replace(s.order_id,'"','')
       ,replace(s.customer_id,'"','')
       ,s.order_status
       ,convert(datetime,s.order_purchase_timestamp)
       ,convert(datetime,s.order_approved_at)
       ,convert(datetime,s.order_delivered_carrier_date)
       ,convert(datetime,s.order_delivered_customer_date)
       ,convert(datetime,s.order_estimated_delivery_date) 
)
when not matched by source
THEN
delete;

--olist.dbo.dim_products
merge olist.dbo.dim_products T
using olist.dbo.stg_products S on t.product_id = s.product_id
when not matched THEN
insert
(       product_sgk
       ,product_id
       ,product_category_name
       ,product_name_lenght
       ,product_description_lenght
       ,product_photos_qty
       ,product_weight_g
       ,product_length_cm
       ,product_height_cm
       ,product_width_cm
)
values 
(       product_sgk
       ,replace(product_id,'"','')
       ,product_category_name
       ,convert(int,product_name_lenght)
       ,convert(int,product_description_lenght)
       ,convert(int,product_photos_qty)
       ,convert(int,product_weight_g)
       ,convert(int,product_length_cm)
       ,convert(int,product_height_cm)
       ,convert(int,product_width_cm)
)when not matched by source
THEN
delete;

-- dim_sellers
merge olist.dbo.dim_sellers T
using olist.dbo.stg_sellers S on t.seller_id = s.seller_id
when not matched THEN
INSERT
(       seller_sgk
       ,seller_id
       ,seller_zip_code_prefix
       ,seller_city
       ,seller_state
)
VALUES
(       seller_sgk
       ,replace(s.seller_id,'"','')
       ,convert(int,replace(s.seller_zip_code_prefix,'"',''))
       ,s.seller_city
       ,replace(replace(seller_state,' rio de janeiro, brasil",RJ','RJ'),' rio grande do sul, brasil",RS','RS')
)when not matched by source
THEN
delete;

-- [dbo].[fato_order_items]
insert into fato_order_items
(
        customer_pk
       ,geolocation_pk
       ,order_items_pk
       ,order_payments_pk
       ,order_reviews_pk
       ,order_pk
       ,product_pk
       ,sellers_pk
       ,Date_sgk
       ,order_id
       ,order_item_id
       ,product_id
       ,seller_id
       ,shipping_limit_date
       ,price
       ,freight_value
)
select 
     a.customer_pk
    ,b.geolocation_pk
    ,c.order_items_pk
    ,d.order_payments_pk
    ,e.order_reviews_pk
    ,f.order_pk
    ,g.product_pk
    ,h.sellers_pk
    ,product_pk as Date_sgk 
    ,f.order_id
    ,order_item_id
    ,g.product_id
    ,h.seller_id
    ,shipping_limit_date
    ,price
    ,freight_value
from olist.dbo.dim_customers a
inner join olist.dbo.dim_geolocation b    on a.customer_sgk = b.geolocation_sgk
inner join olist.dbo.dim_order_items c    on a.customer_sgk = c.order_items_sgk
inner join olist.dbo.dim_order_payments d on a.customer_sgk = d.order_payments_sgk
inner join olist.dbo.dim_order_reviews e  on a.customer_sgk = e.reviews_sgk
inner join olist.dbo.dim_orders f         on a.customer_sgk = f.order_sgk
inner join olist.dbo.dim_products g       on a.customer_sgk = g.product_sgk
inner join olist.dbo.dim_sellers h        on a.customer_sgk = h.seller_sgk



select * from fato_order_items

