# SQL queries for the Data Warehouse wrangling
# Truncate stg.tables
sql_stg_review_trunc 	        = """truncate table stg.review;"""
sql_stg_product_trunc 	        = """truncate table stg.product;"""
sql_stg_product_category_trunc	= """truncate table stg.product_category;"""
sql_stg_country_trunc	        = """truncate table stg.country;"""
sql_stg_marketplace_trunc	    = """truncate table stg.marketplace;"""
sql_stg_customer_trunc 	        = """truncate table stg.customer;"""

truncate_stg_table_queries = [sql_stg_review_trunc, sql_stg_product_trunc, sql_stg_product_category_trunc, sql_stg_country_trunc,
                              sql_stg_marketplace_trunc, sql_stg_customer_trunc]

# Copy from S3 to the Stage layer

sql_stg_copy_template = """
copy stg.{target_table} 
from 's3://{bucket_name}/{key}/'
    iam_role '{iam_role}'
    region '{region_name}'
    format as csv
    compupdate off 
    delimiter ';'
    null as  '\\000'
    ignoreheader 1
"""

stg_copy_object = []
stg_copy_object.append( {"object": "review", "target_table": "review", "key": "review" } )
stg_copy_object.append( {"object": "product", "target_table": "product", "key": "product" } )
stg_copy_object.append( {"object": "country", "target_table": "country", "key": "country"} )
stg_copy_object.append( {"object": "marketplace", "target_table": "marketplace", "key": "marketplace"} )
stg_copy_object.append( {"object": "product", "target_table": "product", "key": "product"} )
stg_copy_object.append( {"object": "customer", "target_table": "customer", "key": "customer"} )
stg_copy_object.append( {"object": "product_category", "target_table": "product_category", "key": "product_category"} )


# Upsert queries

sql_dds_fct_review_upsert = """
begin transaction;

delete from dds.fct_review
using stg.review
where dds.fct_review.review_id = stg.review.review_id;

insert into dds.fct_review (review_id, customer_id, helpful_votes, marketplace, product_category_code, product_id, review_body, 
    review_date, review_headline, star_rating, total_votes, verified_purchase, vine)
select review_id, customer_id, helpful_votes, marketplace, product_category, product_id, review_body, 
    review_date, review_headline, star_rating, total_votes, verified_purchase, vine 
from stg.review;

end transaction ;
commit;
"""

sql_dds_dim_country_upsert = """
begin transaction;

delete from dds.dim_country
using (
    select c.country_code
    from stg.country c
    union 
    select m.marketplace_code as country_code
    from stg.marketplace m
    left join stg.country c on m.marketplace_code = c.country_code
    where c.country_code is null
) src
where dds.dim_country.country_code = src.country_code;

insert into dds.dim_country (country_code, country_name, continent_code, continent_name)
select country_code, country_name, continent_code, continent_name 
from (
    select c.country_code
        , c.country_name
        , c.continent_code
        , c.continent_name
    from stg.country c
    union 
    select m.marketplace_code
        , 'None' as country_name
        , null as continent_code
        , null as continent_name
    from stg.marketplace m
    left join stg.country c on m.marketplace_code = c.country_code
    where c.country_code is null
);

end transaction ;
commit;
"""

sql_dds_dim_customer_upsert = """
begin transaction;

delete from dds.dim_customer
using stg.customer
where dds.dim_customer.customer_id = stg.customer.customer_id;

insert into dds.dim_customer (customer_id)
select customer_id from stg.review;

end transaction ;
commit;
"""

sql_dds_dim_product_upsert = """
begin transaction;

delete from dds.dim_product
using stg.product
where dds.dim_product.product_id = stg.product.product_id;

insert into dds.dim_product (product_id, product_parent, product_title, category_code) 
select product_id, product_parent, product_title, category_code from stg.product;

end transaction ;
commit;
"""

sql_dds_dim_product_category_upsert = """
begin transaction;

delete from dds.dim_product_category
using stg.product_category
where dds.dim_product_category.category_code = stg.product_category.category_code;

insert into dds.dim_product_category (category_code, category_name) 
select category_code, category_name from stg.product_category;

end transaction ;
commit;
"""

upsert_dds_table_queries = [sql_dds_fct_review_upsert, sql_dds_dim_country_upsert, sql_dds_dim_customer_upsert,
                            sql_dds_dim_product_upsert, sql_dds_dim_product_category_upsert]

# DM

sql_dm_product_rating_day_upsert = """
begin transaction;

delete from dm.product_rating_day
using (select distinct review_date as cdate from stg.review) src
where dm.product_rating_day.cdate = src.cdate;

insert into dm.product_rating_day (cdate, product_id, marketplace,  
    star_rating_cnt_1, star_rating_cnt_2, star_rating_cnt_3, star_rating_cnt_4, star_rating_cnt_5, 
    star_helpful_votes_cnt_1, star_helpful_votes_cnt_2, star_helpful_votes_cnt_3, star_helpful_votes_cnt_4, star_helpful_votes_cnt_5, 
    rating_cnt, helpful_votes_cnt, verified_purchase_cnt, is_vine_member_cnt)
select t.review_date as cdate
    , t.product_id
    , t.marketplace
    , sum(decode(t.star_rating, 1, 1, 0)) as star_rating_cnt_1
    , sum(decode(t.star_rating, 2, 1, 0)) as star_rating_cnt_2
    , sum(decode(t.star_rating, 3, 1, 0)) as star_rating_cnt_3
    , sum(decode(t.star_rating, 4, 1, 0)) as star_rating_cnt_4
    , sum(decode(t.star_rating, 5, 1, 0)) as star_rating_cnt_5
    , sum(decode(t.star_rating, 1, t.helpful_votes, 0)) as star_helpful_votes_cnt_1
    , sum(decode(t.star_rating, 2, t.helpful_votes, 0)) as star_helpful_votes_cnt_2
    , sum(decode(t.star_rating, 3, t.helpful_votes, 0)) as star_helpful_votes_cnt_3
    , sum(decode(t.star_rating, 4, t.helpful_votes, 0)) as star_helpful_votes_cnt_4
    , sum(decode(t.star_rating, 5, t.helpful_votes, 0)) as star_helpful_votes_cnt_5
    , sum(case when t.star_rating between 1 and 5 then 1 else 0 end) as rating_cnt
    , sum(t.helpful_votes) as helpful_votes_cnt
    , sum(decode(t.verified_purchase, 'Y', 1, 0)) as verified_purchase_cnt
    , sum(decode(t.vine, 'Y', 1, 0)) as is_vine_member_cnt
from dds.fct_review t
where t.review_date in (select distinct review_date from stg.review)
group by t.review_date
    , t.product_id
    , t.marketplace
;
end transaction ;
commit;
"""

upsert_dm_table_queries = [sql_dm_product_rating_day_upsert, ]
