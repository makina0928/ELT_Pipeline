{% set train_start = '2022-01-01' %}
{% set train_end = '2024-12-31' %}
{% set observation_window_months = 6 %}

WITH training AS (
    SELECT
        -- Updated ID columns for consistency
        f.fact_customer_id,
        f.dim_customer_id,
        f.fact_branch_id,
        f.dim_branch_id,

        -- Loan details
        f.loan_id,
        f.loan_product,
        f.disbursement_date,
        f.disbursed_amt,
        f.outstanding_bal,
        f.interest_rate,

        -- Customer attributes
        f.gender,
        f.marital_status,
        f.employment_status,
        f.income_bracket,

        -- Branch attributes
        f.region,
        f.branch_type,

        -- Loan performance metrics (features)
        f.days_late,
        f.par_0,
        f.par_30,
        f.par_60,
        f.par_90,

        -- Target variable (label)
        f.default_status AS target_default,

        -- Derived date fields
        f.disbursement_month,
        f.disbursement_year

    FROM {{ ref('analytics') }} AS f
    WHERE
        f.disbursement_date BETWEEN '{{ train_start }}' AND '{{ train_end }}'
        -- Ensure at least full observation period before today
        AND f.disbursement_date <= CURRENT_DATE - INTERVAL '{{ observation_window_months }} months'
)

SELECT *
FROM training
