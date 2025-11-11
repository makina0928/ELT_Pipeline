with customers as (
    select * 
    from {{ source('portfolio_data', 'customers') }}
)

select
    -- Convert customer ID to lowercase, ensure consistent format
    TRIM(UPPER(customer_id)) as customer_id,
    
    -- Clean and standardize customer name
    initcap(trim(customer_name)) as customer_name,
    
    -- Normalize gender
    TRIM(UPPER(gender)) as gender,

    -- standardize date of birth
    CASE
        WHEN dob::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN dob::date
        ELSE NULL
    END AS dob,

    -- Clean and standardize categorical fields
    trim(initcap(marital_status)) as marital_status,
    trim(initcap(employment_status)) as employment_status,
    
    -- Normalize income bracket
    trim(income_bracket) as income_bracket,
    
    -- Format region names uniformly
    trim(initcap(region)) as region,
    
    -- Cast credit score to integer
    cast(credit_score as int) as credit_score
    
from customers


-- with customers as (select * from {{ source('portfolio_data', 'customers') }})
-- select
--     cast(loan_account_number as int) as loan_number,
--     initcap(trim(customer_name)) as customer_name,
--     upper(trim(gender)) as gender,
--     upper(trim(industry_description)) as industry_description
-- from customers