with loans as (
    select *
    from {{ ref('src_loans') }}
      where loan_id is not null  -- Exclude invalid records
      and trim(cast(loan_id as text)) <> ''   -- Exclude empty strings if any
)
select
    -- create a consistent surrogate key
    {{ dbt_utils.generate_surrogate_key(['loan_id']) }} as sur_loan_id,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as sur_customer_id,
    {{ dbt_utils.generate_surrogate_key(['branch_id']) }} as sur_branch_id,

    -- clean and standardized fields
    loan_id,
    customer_id,
    branch_id,
    loan_product,
    disbursement_date,
    maturity_date,
    loan_amount,
    interest_rate,
    installment_amount,
    outstanding_balance,
    loan_status,
    days_past_due as days_late,
    CASE WHEN days_past_due < 1 THEN days_past_due ELSE NULL END AS PAR_0,
    CASE WHEN days_past_due >= 1 AND days_past_due < 30 THEN days_past_due ELSE NULL END AS PAR_30,
    CASE WHEN days_past_due >= 30 AND days_past_due < 60 THEN days_past_due ELSE NULL END AS PAR_60,
    CASE WHEN days_past_due >= 60 THEN days_past_due ELSE NULL END AS PAR_90,
    amount_overdue,
    delinquency_start_date,
    default_flag
from loans

