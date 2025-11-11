-- Inference set: NEW loans disbursed in November 2025
-- These loans have no target label (yet)
-- Only include features known at or before disbursement

{% set inference_month_start = '2025-11-01' %}
{% set inference_month_end = '2025-11-30' %}

WITH inference AS (
    SELECT
        -- Updated IDs (match the analytics model)
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

        --  Customer attributes
        f.gender,
        f.marital_status,
        f.employment_status,
        f.income_bracket,

        -- Branch attributes
        f.region,
        f.branch_type,

        -- Derived date columns
        f.disbursement_month,
        f.disbursement_year

    FROM {{ ref('analytics') }} AS f
    WHERE
        f.disbursement_date BETWEEN '{{ inference_month_start }}' AND '{{ inference_month_end }}'
        -- Exclude any loans already defaulted (we only want active/new)
        AND f.default_status = 0
)

SELECT *
FROM inference
