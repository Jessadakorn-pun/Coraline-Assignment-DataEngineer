-- define a model that selects all records from the food_sales source table
select * 
from {{ source('challenge', 'food_sales') }}