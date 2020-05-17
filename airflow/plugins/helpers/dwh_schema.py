from pyspark.sql.types import StructField, StructType, StringType as Str, IntegerType as Int, DateType as Date, ShortType as Short

# DDS SCHEMA

## Drop schemas
sql_stg_schema_drop = """drop schema if exists stg cascade;"""
sql_dds_schema_drop = """drop schema if exists dds cascade;"""
sql_dm_schema_drop  = """drop schema if exists dm cascade;"""
sql_etl_schema_drop = """drop schema if exists etl cascade;"""
drop_schema_queries = [sql_stg_schema_drop, sql_dds_schema_drop, sql_dm_schema_drop, sql_etl_schema_drop]


## Create schemas
sql_stg_schema  = """create schema if not exists stg;"""
sql_dds_schema  = """create schema if not exists dds;"""
sql_dm_schema   = """create schema if not exists dm;"""
sql_etl_schema  = """create schema if not exists etl;"""
create_schema_queries = [sql_stg_schema, sql_dds_schema, sql_dm_schema, sql_etl_schema]


# TABLES

## Create stg.tables
sql_stg_review = """
create table if not exists stg.review (
	marketplace 		char(2), 
	customer_id 		bigint, 
	review_id 			varchar, 
	product_id 			varchar, 
	product_parent 		bigint, 
	product_title 		varchar(2048), 
	star_rating 		smallint, 
	helpful_votes 		int, 
	total_votes 		int, 
	vine 				char(1), 
	verified_purchase 	char(1), 
	review_headline 	varchar, 
	review_body 		varchar(max), 
	review_date 		date,
	year                smallint,
	product_category    varchar
);
"""

sql_stg_product = """
create table if not exists stg.product (
	product_id 			varchar, 
	product_parent 		bigint, 
	product_title 		varchar(2048),
	category_code 	    varchar
);
"""

sql_stg_product_category = """
create table if not exists stg.product_category (
	category_code   varchar,
	category_name   varchar
);
"""

sql_stg_country = """
create table if not exists stg.country (
	country_code 	            char(2),
	country_name				varchar,
	continent_code				char(2),
	continent_name				varchar
);
"""

sql_stg_marketplace = """
create table if not exists stg.marketplace (
	marketplace_code    char(2)
);
"""

sql_stg_customer = """
create table if not exists stg.customer (
	customer_id bigint
);
"""
create_stg_table_queries = [sql_stg_review, sql_stg_product, sql_stg_product_category, sql_stg_country, sql_stg_marketplace, sql_stg_customer]


## Create dds.tables
sql_dds_country = """
create table if not exists dds.dim_country (
	country_code 	char(2) primary key,
	country_name	varchar,
	continent_code	char(2),
	continent_name	varchar
)
diststyle all
sortkey(country_code);
"""

sql_dds_customer = """
create table if not exists dds.dim_customer (
	customer_id     bigint primary key
)
sortkey(customer_id);
"""

sql_dds_product = """
create table if not exists dds.dim_product (
	product_id 		varchar primary key, 
	product_parent  bigint, 
	product_title 	varchar(2048),
	category_code 	varchar
)
diststyle all
sortkey(product_id);
"""

sql_dds_product_category = """
create table if not exists dds.dim_product_category (
	category_code   varchar primary key, 
	category_name   varchar
)
diststyle all;
"""

sql_dds_rating = """
create table if not exists dds.dim_rating (
	star_rating     smallint sortkey, 
	rating_title    varchar
)
diststyle all;
"""

sql_dds_calendar = """
create table if not exists dds.dim_calendar (
	cdate				date primary key,
	day					char(2),
	day_of_week			smallint,
	day_of_week_name	varchar(14),
	month				char(2),
	month_name			varchar(14),
	month_short_name	char(3),
	year				smallint
)
diststyle all
sortkey(cdate);
"""

sql_dds_review = """
create table if not exists dds.fct_review (
	marketplace 		    char(2), 
	customer_id 		    bigint, 
	review_id 			    varchar primary key, 
	product_id 			    varchar, 
	product_category_code 	varchar,
	star_rating 		    smallint, 
	helpful_votes 		    int, 
	total_votes 		    int, 
	vine 				    char(1), 
	verified_purchase 	    char(1), 
	review_headline 	    varchar, 
	review_body 		    varchar(max),
	review_date 		    date
)
distkey(product_id)
sortkey(review_date)
;
"""
create_dds_table_queries = [sql_dds_country, sql_dds_customer, sql_dds_product, sql_dds_product_category, sql_dds_rating, sql_dds_calendar, sql_dds_review]

## Create dm.tables

sql_dm_product_rating = """
create table if not exists dm.product_rating_day (
    cdate                       date,
    product_id                  varchar,
    marketplace                 char(2),
    star_rating_cnt_1           int,
    star_rating_cnt_2           int,
    star_rating_cnt_3           int,
    star_rating_cnt_4           int,
    star_rating_cnt_5           int,
    star_helpful_votes_cnt_1    int,
    star_helpful_votes_cnt_2    int,
    star_helpful_votes_cnt_3    int,
    star_helpful_votes_cnt_4    int,
    star_helpful_votes_cnt_5    int,
    rating_cnt                  int,
    helpful_votes_cnt           int,
    verified_purchase_cnt       int,
    is_vine_member_cnt          int
)
diststyle auto 
distkey(product_id)
sortkey(cdate);
"""

create_dm_table_queries = [sql_dm_product_rating, ]

## Create etl.tables
sql_etl_check_dw = """
create table if not exists etl.check_dq (
    id          bigint identity,
    cdate       timestamp,
    check_type  smallint,
    check_name  varchar,
    query       varchar(2048),
    param       varchar(2048),
    row_number  int
)
distkey(id)
sortkey(cdate)
;
"""

create_etl_table_queries = [sql_etl_check_dw, ]