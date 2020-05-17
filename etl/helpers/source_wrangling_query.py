# SQL queries for Working Zone

country_working_zone_insert = """
    with src as (
        select upper(Two_Letter_Country_Code) as country_code 
            , Country_Name as country_name
            , Continent_Code as continent_code
            , Continent_Name as continent_name
            , row_number() over (partition by Two_Letter_Country_Code order by Country_Name) as rn
        from country
        where Two_Letter_Country_Code is not null
    )
    select country_code
        , country_name
        , continent_code
        , continent_name
    from src
    where rn = 1
"""

product_working_zone_insert = """
    with src as (
        select product_id
            , product_parent
            , product_title
            , product_category as category_code
            , row_number() over (partition by product_id order by review_date desc) as rn
        from product
        where product_id is not null
    )
    select product_id
        , product_parent
        , product_title
        , category_code
    from src
    where rn = 1
"""