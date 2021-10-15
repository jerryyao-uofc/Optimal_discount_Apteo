SQL_TRANSACTIONS_AND_CUSTOMER_EMAIL_MACTCHING = r"""
WITH  basic_info AS (
    SELECT 
    email_address,
    customer_id,
  FROM
    -- HIDDEN TABLE ID
),

transactions_data AS(

SELECT 
    order_id,
    customer_id,
    created_at,
    line_items,
    total_amount, 
    discount_amount,
    (discount_amount)/(total_amount + discount_amount) AS discount_percentage,
    total_amount_currency,
    shipping_address_zip_code,
    shipping_address_city,
    shipping_address_state,
    shipping_address_country
  FROM
     -- HIDDEN TABLE ID
  
  WHERE
     total_amount>0
)

SELECT *
FROM transactions_data 
LEFT JOIN basic_info on transactions_data.customer_id = basic_info.customer_id
ORDER BY transactions_data.created_at

"""



SQL_CUSTOMER_QUERY = r"""SELECT email_address, customer_id
from -- HIDDEN TABLE ID"""