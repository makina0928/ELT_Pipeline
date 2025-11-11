with loans as (select * from {{ source('portfolio_data', 'loans') }})

SELECT
    CAST(loan_id AS VARCHAR) AS loan_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(branch_id AS VARCHAR) AS branch_id,
    CAST(loan_product AS VARCHAR) AS loan_product,

    CASE
        WHEN disbursement_date::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN disbursement_date::date
        ELSE NULL
    END AS disbursement_date,

    CASE
        WHEN maturity_date::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN maturity_date::date
        ELSE NULL
    END AS maturity_date,

    CAST(loan_amount AS NUMERIC(12,2)) AS loan_amount,
    CAST(interest_rate AS NUMERIC(5,2)) AS interest_rate,
    CAST(installment_amount AS NUMERIC(12,2)) AS installment_amount,
    CAST(outstanding_balance AS NUMERIC(12,2)) AS outstanding_balance,
    CAST(loan_status AS VARCHAR) AS loan_status,
    CAST(days_past_due AS NUMERIC(6,2)) AS days_past_due,
    CAST(amount_overdue AS NUMERIC(12,2)) AS amount_overdue,

    CASE
        WHEN delinquency_start_date::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN delinquency_start_date::date
        ELSE NULL
    END AS delinquency_start_date,

    CAST(default_flag AS INT) AS default_flag
FROM loans
