SELECT CUSTOMER, YEAR_MONTH,
COALESCE(total_balance_max - LAG(total_balance_max) OVER ( PARTITION BY CUSTOMER ORDER BY YEAR_MONTH DESC ), 
total_balance_max-total_balance_min) COST_LAST_MONTH,
total_balance_max
FROM
(
SELECT 
sold_to_customer_name AS CUSTOMER,
CAST(DATE_PART(YEAR, DATE) || '-' || RIGHT( '0' || DATE_PART(MONTH, DATE),2) || '-01' AS DATE) YEAR_MONTH,
MAX(cast(capacity_balance + free_usage_balance + rollover_balance as decimal (38,2))) as total_balance_max,
MIN(cast(capacity_balance + free_usage_balance + rollover_balance as decimal (38,2))) as total_balance_min
from snowflake.billing_usage.remaining_balance_daily
where 1 = 1 
and DATE > '2022-04-01'
group by 1, 2
) A
;
