-- Testing/Evaluation set: loans disbursed from 2025-01-01 to 2025-10-01
-- Must have full 6-month observation window

{% set test_start = '2025-01-01' %}
{% set test_end = '2025-10-01' %}
{% set observation_window_months = 6 %}

WITH testing AS (
    SELECT
        -- Updated ID columns for consistency
        f.fact_customer_id,
        f.dim_customer_id,
        f.fact_branch_id,
        f.dim_branch_id,

        -- Core loan attributes
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

        -- Performance metrics and target label
        f.days_late,
        f.par_0,
        f.par_30,
        f.par_60,
        f.par_90,
        f.default_status AS target_default,

        --  Derived date fields
        f.disbursement_month,
        f.disbursement_year

    FROM {{ ref('analytics') }} AS f
    WHERE 
        f.disbursement_date BETWEEN '{{ test_start }}' AND '{{ test_end }}'
        -- Ensure full observation period (e.g., at least 6 months since disbursement)
        AND f.disbursement_date <= CURRENT_DATE - INTERVAL '{{ observation_window_months }} months'
)

SELECT *
FROM testing
