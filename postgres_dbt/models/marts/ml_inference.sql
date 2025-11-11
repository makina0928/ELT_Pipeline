-- Inference set: NEW loans disbursed in November 2025
-- These loans have no target label (yet)
-- Only include features known at or before disbursement

{% set inference_month_start = '2025-11-01' %}
{% set inference_month_end = '2025-11-30' %}

WITH inference AS (
    SELECT
        sur_customer_id,
        loan_id,
        loan_product,
        disbursement_date,
        disbursed_amt,
        outstanding_bal,
        interest_rate,
        gender,
        marital_status,
        employment_status,
        income_bracket,
        region,
        branch_type,
        disbursement_month,
        disbursement_year
    FROM {{ ref('analytics') }}
    WHERE
        disbursement_date BETWEEN '{{ inference_month_start }}' AND '{{ inference_month_end }}'
        -- Exclude any loans already defaulted (we only want active/new)
        AND default_status = 0
)

SELECT *
FROM inference