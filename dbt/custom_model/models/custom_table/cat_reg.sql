select
    category
    , round(sum(case when region = 'East' then totalprice::numeric else 0 end)) as East
    , round(sum(case when region = 'West' then totalprice::numeric else 0 end)) as West
    , round(sum(totalprice)) as Grand_Total
from {{ ref('food_sales') }}
group by category
order by category