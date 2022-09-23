/*********************************************
-- assunto= criação das stage areas + carga
-- autor =  eduardo alberto
-- data  =  21/04/2021
**********************************************/

-- CHECK IF DATABASE EXIST
IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'olist')
CREATE DATABASE olist

--CREATE TABLES STAGE
/*
    olist.dbo.stg_customers
*/
drop table if exists olist.dbo.stg_customers
create table olist.dbo.stg_customers(
 customer_id        varchar(max)
,customer_unique_id varchar(max)
,customer_zip_code_prefix varchar(max)
,customer_city      varchar(max)
,customer_state     varchar(max)
);

-- bulk insert olist.dbo.stg_customers
bulk insert olist.dbo.stg_customers
from '/var/opt/mssql/data/olist_customers_dataset.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    KEEPIDENTITY

)
go
-- add new column identity
alter table olist.dbo.stg_customers add customer_sgk  INT Identity(1,1)

/*
    olist.dbo.stg_geolocation
*/
drop table if exists olist.dbo.stg_geolocation
create table olist.dbo.stg_geolocation(
     geolocation_zip_code_prefix varchar(max)
    ,geolocation_lat             varchar(max)   
    ,geolocation_lng             varchar(max)   
    ,geolocation_city            varchar(max)   
    ,geolocation_state           varchar(max)
);

-- bulk insert olist.dbo.stg_geolocation
bulk insert olist.dbo.stg_geolocation
from '/var/opt/mssql/data/olist_geolocation_dataset.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
)
go

-- add new column identity
alter table olist.dbo.stg_geolocation add geolocation_sgk  INT Identity(1,1)
go


/*
    olist.dbo.stg_order_items
*/

drop table if exists olist.dbo.stg_order_items
create table olist.dbo.stg_order_items(
    order_id        varchar(max)
    ,order_item_id  varchar(max)
    ,product_id     varchar(max)
    ,seller_id      varchar(max)
    ,shipping_limit_date varchar(max)
    ,price          varchar(max)
    ,freight_value  varchar(max)
);

-- bulk insert stg_order_items
bulk insert olist.dbo.stg_order_items
from '/var/opt/mssql/data/olist_order_items_dataset.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
)
go

-- add new column identity
alter table olist.dbo.stg_order_items add order_items_sgk  INT Identity(1,1)
go

drop table if exists olist.dbo.stg_order_payments
create table olist.dbo.stg_order_payments(
     order_id               varchar(max)     
    ,payment_sequential     varchar(max)
    ,payment_type           varchar(max)   
    ,payment_installments   varchar(max)
    ,payment_value          varchar(max)
);

-- bulk insert stg_order_payments
bulk insert olist.dbo.stg_order_payments
from '/var/opt/mssql/data/olist_order_payments_dataset.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
)
go

-- add new column identity
alter table olist.dbo.stg_order_payments add order_payments_sgk  INT Identity(1,1)
go

drop table if exists olist.dbo.stg_order_reviews
create table olist.dbo.stg_order_reviews(
     review_id               varchar(max)
    ,order_id                varchar(max)
    ,review_score            varchar(max)
    ,review_comment_title    varchar(max)
    ,review_comment_message  varchar(max)
    ,review_creation_date    varchar(max)
    ,review_answer_timestamp varchar(max)
);

-------/* carga da tabela stg_order_reviews no python */------------
-- bulk insert teste
-- from '/var/opt/mssql/data/olist_order_reviews_dataset.csv'
-- with
-- (   

--     firstrow = 2,
--     fieldterminator = ',',
--     rowterminator = '\n',
--     tablock
-- )
-- go

-- add new column identity
alter table olist.dbo.stg_order_reviews add reviews_sgk  INT Identity(1,1)
go

-- stage olist.dbo.stg_orders
drop table if exists olist.dbo.stg_orders
create table olist.dbo.stg_orders(
     order_id                       varchar(max)
    ,customer_id                    varchar(max)
    ,order_status                   varchar(max)
    ,order_purchase_timestamp       varchar(max)
    ,order_approved_at              varchar(max)
    ,order_delivered_carrier_date   varchar(max)
    ,order_delivered_customer_date  varchar(max)
    ,order_estimated_delivery_date  varchar(max)
);

-- bulk insert stg_orders
bulk insert olist.dbo.stg_orders
from '/var/opt/mssql/data/olist_orders_dataset.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
)
go

-- add new column identity
alter table olist.dbo.stg_orders add order_sgk  INT Identity(1,1)
go


-- bulk insert stg_products
drop table if exists olist.dbo.stg_products
create table olist.dbo.stg_products(
     product_id                     varchar(max)
    ,product_category_name          varchar(max)
    ,product_name_lenght            varchar(max)
    ,product_description_lenght     varchar(max)
    ,product_photos_qty             varchar(max)
    ,product_weight_g               varchar(max)
    ,product_length_cm              varchar(max)
    ,product_height_cm              varchar(max)
    ,product_width_cm               varchar(max)

);

-- bulk insert stg_products
bulk insert olist.dbo.stg_products
from '/var/opt/mssql/data/olist_products_dataset.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
)
go
-- add new column identity
alter table olist.dbo.stg_products add product_sgk  INT Identity(1,1)
go

-- stg_sellers
drop table if exists olist.dbo.stg_sellers
create table olist.dbo.stg_sellers(
     seller_id                  varchar(max)
    ,seller_zip_code_prefix     varchar(max)
    ,seller_city                varchar(max)
    ,seller_state               varchar(max)
);
-- bulk insert stg_sellers
bulk insert olist.dbo.stg_sellers
from '/var/opt/mssql/data/olist_sellers_dataset.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
)
go

-- add new column identity
alter table olist.dbo.stg_sellers add seller_sgk  INT Identity(1,1)
go


-- stg_category_name_translation
drop table if exists olist.dbo.stg_category_name_translation
create table olist.dbo.stg_category_name_translation(
    product_category_name       varchar(max)
    ,product_category_name_english varchar(max)
);

-- bulk insert stg_category_name_translation
bulk insert olist.dbo.stg_category_name_translation
from '/var/opt/mssql/data/product_category_name_translation.csv'
with
(   
    firstrow = 2,
    fieldterminator = ',',
    rowterminator = '\n',
    tablock
)
go

-- add new column identity
alter table olist.dbo.stg_category_name_translation add product_category_sgk  INT Identity(1,1)
go

