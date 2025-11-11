WITH 
    -- Dimension tables
    dim_customers AS (
        SELECT
            sur_customer_id,         -- surrogate customer key
            customer_name,
            gender,
            dob,
            marital_status,
            employment_status,
            income_bracket
        FROM {{ ref('customers') }}
    ),

    dim_location AS (
        SELECT
            branch_id_sur,
            branch_name,
            region,
            country,
            manager_name,
            opened_date,
            branch_type
        FROM {{ ref('location') }}
    ),

    -- Fact table (main transactional / loan-level data)
    fact_loans AS (
        SELECT
            sur_loan_id,             -- surrogate loan key (unique loan-level)
            loan_id,                 -- natural loan identifier
            sur_customer_id,         -- foreign key to dim_customers
            sur_branch_id,           -- foreign key to dim_location
            loan_product,
            disbursement_date,
            maturity_date,
            loan_amount AS disbursed_amt,
            outstanding_balance AS outstanding_bal,
            loan_status,
            interest_rate,
            days_late,
            par_0,
            par_30,
            par_60,
            par_90,
            amount_overdue,
            delinquency_start_date
        FROM {{ ref('fact_tables') }}
    )

-- === STAR JOIN ===
SELECT 
    c.sur_customer_id,
    c.customer_name,
    c.gender,
    c.marital_status,
    c.employment_status,
    c.income_bracket,
    o.branch_id_sur,
    o.branch_name,
    o.region,
    o.country,
    o.manager_name,
    o.branch_type,
    f.sur_branch_id,
    f.loan_id,
    f.loan_product,
    f.disbursement_date,
    f.maturity_date,
    f.disbursed_amt,
    f.outstanding_bal,
    f.interest_rate,
    f.loan_status,
    f.days_late,
    f.par_0,
    f.par_30,
    f.par_60,
    f.par_90,
    f.amount_overdue,
    f.delinquency_start_date,

    -- Loan status calculations
    CASE 
        WHEN f.days_late >= 60 THEN 1
        ELSE 0
    END AS default_status,

    -- Portfolio at Risk (PAR) ratio
    ROUND(COALESCE(f.par_30 + f.par_60 + f.par_90, 0)::NUMERIC / NULLIF(f.outstanding_bal, 0), 4) AS portfolio_at_risk_ratio,

    -- Derived date columns
    EXTRACT(YEAR FROM f.disbursement_date) AS disbursement_year,
    EXTRACT(MONTH FROM f.disbursement_date) AS disbursement_month,

    -- New ranking column:
    DENSE_RANK() OVER (
        ORDER BY 
            EXTRACT(YEAR FROM f.disbursement_date),
            EXTRACT(MONTH FROM f.disbursement_date)
    ) AS disbursement_month_rank

FROM fact_loans f
LEFT JOIN dim_customers c 
    ON f.sur_customer_id = c.sur_customer_id
LEFT JOIN dim_location o 
    ON f.sur_branch_id = o.branch_id_sur