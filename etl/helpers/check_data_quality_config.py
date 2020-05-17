# Configuration for data quality module
#
#
#
#

# SQL query template for etl.check_dq table
check_dq_insert = """
insert into etl.check_dq (cdate, check_type, check_name, query, param, row_number)
values(getdate(), {check_type}, '{check_name}', '{query}', '{param}', {row_number});
"""

# SQL query for checking 1
query_dq_type_1 = """
select count(*) from {schema}.{table} where {field} is null;
"""

# SQL query for checking 2
query_dq_type_2 = """
select count(*) from {fct_schema}.{fct_table} f
left join {dim_schema}.{dim_table} d on f.{fct_field} = d.{dim_field}
where d.{dim_field} is null;
"""

# SQL query for checking 3
query_dq_type_3 = """
select count(*) from {schema}.{table};
"""

# objects for checking 1
object_dq_type_1 = []
object_dq_type_1.append( {"schema": "stg", "table": "review", "field": "customer_id"} )
object_dq_type_1.append( {"schema": "stg", "table": "review", "field": "marketplace"} )
object_dq_type_1.append( {"schema": "stg", "table": "review", "field": "product_id"} )
object_dq_type_1.append( {"schema": "stg", "table": "review", "field": "customer_id"} )

# objects for checking 2
object_dq_type_2 = []
object_dq_type_2.append( {"fct_schema": "dds", "fct_table": "fct_review",
                          "dim_schema": "dds", "dim_table": "dim_customer",
                          "fct_field":  "customer_id", "dim_field": "customer_id"} )
object_dq_type_2.append( {"fct_schema": "dds", "fct_table": "fct_review",
                          "dim_schema": "dds", "dim_table": "dim_product",
                          "fct_field":  "product_id", "dim_field": "product_id"} )

# objects for checking 3
object_dq_type_3 = []
object_dq_type_3.append( {"schema": "dds", "table": "dim_country"} )
object_dq_type_3.append( {"schema": "dds", "table": "dim_customer"} )
object_dq_type_3.append( {"schema": "dds", "table": "dim_product"} )
object_dq_type_3.append( {"schema": "dds", "table": "dim_product_category"} )
object_dq_type_3.append( {"schema": "dds", "table": "dim_rating"} )
object_dq_type_3.append( {"schema": "dds", "table": "fct_review"} )
object_dq_type_3.append( {"schema": "dm", "table": "product_rating_day"} )


check_dq_type = []
check_dq_type.append(
    { "check_type": 1,
     "check_name": "Check NULL values in the Foreign key fields",
     "query_template": query_dq_type_1,
     "check_object": object_dq_type_1
    }
)
check_dq_type.append(
    { "check_type": 2,
     "check_name": "Check non existing records in the Dimensions tables",
     "query_template": query_dq_type_2,
     "check_object": object_dq_type_2
    }
)
check_dq_type.append(
    { "check_type": 3,
     "check_name": "Check records count",
     "query_template": query_dq_type_3,
     "check_object": object_dq_type_3
    }
)