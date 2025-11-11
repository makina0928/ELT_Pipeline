with customers as (
    select *
    from {{ ref('src_customer') }}
    where customer_id is not null  -- Exclude invalid records
      and trim(cast(customer_id as text)) <> ''   -- Exclude empty strings if any
)
select
    -- create a consistent surrogate key
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as sur_customer_id,

    -- clean and standardized fields
    customer_id,
    initcap(trim(customer_name)) as customer_name,
    case 
        when gender = 'MALE' then 'Male'
        when gender = 'FEMALE' then 'Female'
        else 'Unknown'
    end as gender,
    dob,
    marital_status,
    employment_status,
    income_bracket,
    region,
    credit_score
from customers
