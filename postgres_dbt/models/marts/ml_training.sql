{% set train_start = '2022-01-01' %}
{% set train_end = '2024-12-31' %}
{% set observation_window_months = 6 %}

WITH training AS (
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
        disbursement_date BETWEEN '{{ train_start }}' AND '{{ train_end }}'
        AND disbursement_date <= CURRENT_DATE - INTERVAL '{{ observation_window_months }} months'
)

SELECT *
FROM training

-- “Give me all loans disbursed between Jan 2022 and Dec 2024,
-- but only if they are at least 6 months old as of today’s date.”
-- WHERE
--     disbursement_date BETWEEN '2022-01-01' AND '2024-12-31'
--     AND disbursement_date <= CURRENT_DATE - INTERVAL '6 months'
