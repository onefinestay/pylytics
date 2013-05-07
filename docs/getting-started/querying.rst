Querying the star schema
========================

Examples
--------
These examples use `examples/count_all_the_sales` in the github repository.

Example 1
*********
Find all the locations which had daily sales greater than 1000::

    SELECT distinct location
    FROM fact_count_all_the_sales fact
    JOIN dim_location dim_l ON fact.dim_location = dim_l.id
    JOIN dim_date dim_d ON fact.dim_date = dim_d.id
    WHERE fact.fact_count > 1000;