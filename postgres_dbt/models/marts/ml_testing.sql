-- Testing/Evaluation set: loans disbursed from 2025-01-01 to 2025-10-01
-- Must have full 6-month observation window

{% set test_start = '2025-01-01' %}
{% set test_end = '2025-10-01' %}
{% set observation_window_months = 6 %}

WITH testing AS (
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
        days_late,
        par_0,
        par_30,
        par_60,
        par_90,
        default_status AS target_default,
        disbursement_month,
        disbursement_year
    FROM {{ ref('analytics') }}
    WHERE 
        disbursement_date BETWEEN '{{ test_start }}' AND '{{ test_end }}'
        -- Ensure full observation period before today
        AND disbursement_date <= CURRENT_DATE - INTERVAL '{{ observation_window_months }} months'
)

SELECT *
FROM testing
