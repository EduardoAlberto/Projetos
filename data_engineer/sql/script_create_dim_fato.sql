/*********************************************
-- assunto= script das tabelas DIMs e Fatos
-- autor =  eduardo alberto
-- data  =  21/04/2021
**********************************************/
-- dim_customers
create table olist.dbo.dim_customers(
     customer_pk  int IDENTITY(1,1)
    ,customer_sgk int
    ,customer_id varchar(max)  
    ,customer_unique_id varchar(max)
    ,customer_zip_code_prefix INT
    ,customer_city varchar(max)
    ,customer_state char(2)
    CONSTRAINT pk_customer PRIMARY KEY (customer_pk, customer_sgk)
)

-- dim_geolocation
create table olist.dbo.dim_geolocation(
     geolocation_pk  int IDENTITY(1,1)
    ,geolocation_sgk int
    ,geolocation_zip_code_prefix INT
    ,geolocation_lat varchar(max)
    ,geolocation_lng varchar(max)
    ,geolocation_city varchar(max)
    ,geolocation_state char(2)
    CONSTRAINT pk_geolocation PRIMARY KEY (geolocation_pk, geolocation_sgk)
)

-- dim_order_items
create table dim_order_items(
     order_items_pk  int IDENTITY(1,1)
    ,order_id        varchar(max)
    ,order_item_id   int
    ,product_id      varchar(max)
    ,seller_id       varchar(max)
    ,shipping_limit_date datetime 
    ,price            float
    ,freight_value    float
    ,order_items_sgk  int 
    CONSTRAINT pk_order_items PRIMARY KEY (order_items_pk, order_items_sgk)
)
-- dim_order_payments
create table olist.dbo.dim_order_payments(
     order_payments_pk  int IDENTITY(1,1)
    ,order_payments_sgk int
    ,order_id varchar(max)
    ,payment_sequential INT
    ,payment_type varchar(max)
    ,payment_installments INT
    ,payment_value float
    CONSTRAINT pk_order_payments PRIMARY KEY (order_payments_pk, order_payments_sgk)
)

-- dim_order_reviews
create table olist.dbo.dim_order_reviews(
     order_reviews_pk  int IDENTITY(1,1)
    ,reviews_sgk int 
    ,review_id varchar(max)
    ,order_id varchar(max)
    ,review_score INT
    ,review_comment_title varchar(max)
    ,review_comment_message varchar(max)
    ,review_creation_date DATETIME
    ,review_answer_timestamp DATETIME
    CONSTRAINT pk_order_reviews PRIMARY KEY (order_reviews_pk, reviews_sgk)
)

-- dim_orders
create table olist.dbo.dim_orders(
     order_pk  int IDENTITY(1,1)   
    ,order_sgk int 
    ,order_id varchar(max)
    ,customer_id varchar(max)
    ,order_status varchar(max)
    ,order_purchase_timestamp DATETIME
    ,order_approved_at DATETIME
    ,order_delivered_carrier_date DATETIME
    ,order_delivered_customer_date DATETIME
    ,order_estimated_delivery_date DATETIME
    CONSTRAINT pk_order PRIMARY KEY (order_pk, order_sgk)
)

-- dim_products
create table olist.dbo.dim_products(
     product_pk  int IDENTITY(1,1)  
    ,product_sgk int
    ,product_id varchar(max)
    ,product_category_name varchar(max)
    ,product_name_lenght INT
    ,product_description_lenght INT
    ,product_photos_qty INT
    ,product_weight_g INT
    ,product_length_cm INT
    ,product_height_cm INT
    ,product_width_cm INT
    CONSTRAINT pk_products PRIMARY KEY (product_pk, product_sgk)
)

-- dim_sellers
create table olist.dbo.dim_sellers(
     sellers_pk  int IDENTITY(1,1)
    ,seller_sgk int
    ,seller_id varchar(max)
    ,seller_zip_code_prefix INT
    ,seller_city varchar(max)
    ,seller_state char(2)
    CONSTRAINT pk_sellers PRIMARY KEY (sellers_pk, seller_sgk)
)


-- Dim_Date
CREATE TABLE olist.dbo.Dim_Date(

	Date_sgk int PRIMARY key not NULL,
	FullDateAlternateKey date NOT NULL,
	DayNumberOfWeek tinyint NOT NULL,
	EnglishDayNameOfWeek varchar(max) NOT NULL,
	SpanishDayNameOfWeek varchar(max)NOT NULL,
	FrenchDayNameOfWeek varchar(max) NOT NULL,
	DayNumberOfMonth tinyint NOT NULL,
	DayNumberOfYear smallint NOT NULL,
	WeekNumberOfYear tinyint NOT NULL,
	EnglishMonthName varchar(max) NOT NULL,
	SpanishMonthName varchar(max) NOT NULL,
	FrenchMonthName varchar(max) NOT NULL,
	MonthNumberOfYear tinyint NOT NULL,
	CalendarQuarter tinyint NOT NULL,
	CalendarYear smallint NOT NULL,
	CalendarSemester tinyint NOT NULL,
	FiscalQuarter tinyint NOT NULL,
	FiscalYear smallint NOT NULL,
	FiscalSemester tinyint NOT NULL
) 


-- fato_order_items
create table olist.dbo.fato_order_items(
     customer_pk            int
    ,geolocation_pk         int
    ,order_items_pk         int
    ,order_payments_pk      int
    ,order_reviews_pk       int
    ,order_pk               int
    ,product_pk             int
    ,sellers_pk             int
    ,Date_sgk               int   
    ,order_id               varchar(max)
    ,order_item_id          int
    ,product_id             varchar(max)
    ,seller_id              varchar(max)
    ,shipping_limit_date    DATETIME
    ,price                  FLOAT
    ,freight_value          FLOAT
  
)

/*
drop table dim_customers
drop table dim_geolocation
drop table dim_order_items
drop table dim_order_payments
drop table dim_order_reviews
drop table dim_orders
drop table dim_products
drop table dim_sellers
drop table Dim_Date
drop table fato_order_items

*/





