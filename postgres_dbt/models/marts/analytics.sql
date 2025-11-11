-- WITH 
--     -- Dimension tables
--     dim_customers AS (
--         SELECT
--             customer_id,
--             -- sur_customer_id,         -- surrogate customer key
--             customer_name,
--             gender,
--             dob,
--             marital_status,
--             employment_status,
--             income_bracket
--         FROM {{ ref('customers') }}
--     ),

--     dim_location AS (
--         SELECT
--             branch_id,
--             -- branch_id_sur,
--             branch_name,
--             region,
--             country,
--             manager_name,
--             opened_date,
--             branch_type
--         FROM {{ ref('location') }}
--     ),

--     -- Fact table (main transactional / loan-level data)
--     fact_loans AS (
--         SELECT
--             -- sur_loan_id,             
--             loan_id, 
--             customer_id, 
--             branch_id,             
--             -- sur_customer_id,         
--             -- sur_branch_id,           
--             loan_product,
--             disbursement_date,
--             maturity_date,
--             loan_amount AS disbursed_amt,
--             outstanding_balance AS outstanding_bal,
--             loan_status,
--             interest_rate,
--             days_late,
--             par_0,
--             par_30,
--             par_60,
--             par_90,
--             amount_overdue,
--             delinquency_start_date
--         FROM {{ ref('fact_tables') }}
--     )

-- -- === STAR JOIN ===
-- SELECT 
--     -- c.sur_customer_id,
--     c.customer_id,
--     c.customer_name,
--     c.gender,
--     c.marital_status,
--     c.employment_status,
--     c.income_bracket,
--     -- o.branch_id_sur,
--     o.branch_id,
--     o.branch_name,
--     o.region,
--     o.country,
--     o.manager_name,
--     o.branch_type,
--     -- f.sur_branch_id,
--     f.loan_id,
--     f.customer_id,
--     f.branch_id,
--     f.loan_product,
--     f.disbursement_date,
--     f.maturity_date,
--     f.disbursed_amt,
--     f.outstanding_bal,
--     f.interest_rate,
--     f.loan_status,
--     f.days_late,
--     f.par_0,
--     f.par_30,
--     f.par_60,
--     f.par_90,
--     f.amount_overdue,
--     f.delinquency_start_date,

--     -- Loan status calculations
--     CASE 
--         WHEN f.days_late >= 60 THEN 1
--         ELSE 0
--     END AS default_status,

--     -- Portfolio at Risk (PAR) ratio
--     ROUND(COALESCE(f.par_30 + f.par_60 + f.par_90, 0)::NUMERIC / NULLIF(f.outstanding_bal, 0), 4) AS portfolio_at_risk_ratio,

--     -- Derived date columns
--     EXTRACT(YEAR FROM f.disbursement_date) AS disbursement_year,
--     EXTRACT(MONTH FROM f.disbursement_date) AS disbursement_month,

--     -- New ranking column:
--     DENSE_RANK() OVER (
--         ORDER BY 
--             EXTRACT(YEAR FROM f.disbursement_date),
--             EXTRACT(MONTH FROM f.disbursement_date)
--     ) AS disbursement_month_rank

-- FROM fact_loans f
-- INNER JOIN dim_customers c 
--     ON f.customer_id = c.customer_id
-- INNER JOIN dim_location o 
--     ON f.branch_id = o.branch_id

WITH 
    -- Dimension tables
    dim_customers AS (
        SELECT
            customer_id,
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
            branch_id,
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
            loan_id, 
            customer_id, 
            branch_id,             
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
    -- Key alignment (with clear lineage)
    f.customer_id AS fact_customer_id,
    c.customer_id AS dim_customer_id,
    f.branch_id AS fact_branch_id,
    o.branch_id AS dim_branch_id,

    -- Core identifiers
    f.loan_id,

    -- Customer attributes
    c.customer_name,
    c.gender,
    c.marital_status,
    c.employment_status,
    c.income_bracket,

    -- Branch / location attributes
    o.branch_name,
    o.region,
    o.country,
    o.manager_name,
    o.branch_type,

    -- Loan / performance metrics
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

    -- Loan status flag
    CASE 
        WHEN f.days_late >= 60 THEN 1
        ELSE 0
    END AS default_status,

    -- Portfolio at Risk (PAR) ratio
    ROUND(
        COALESCE(f.par_30 + f.par_60 + f.par_90, 0)::NUMERIC 
        / NULLIF(f.outstanding_bal, 0), 
        4
    ) AS portfolio_at_risk_ratio,

    -- Derived time dimensions
    EXTRACT(YEAR FROM f.disbursement_date) AS disbursement_year,
    EXTRACT(MONTH FROM f.disbursement_date) AS disbursement_month,

    -- Ranking by disbursement period
    DENSE_RANK() OVER (
        ORDER BY 
            EXTRACT(YEAR FROM f.disbursement_date),
            EXTRACT(MONTH FROM f.disbursement_date)
    ) AS disbursement_month_rank

FROM fact_loans f
INNER JOIN dim_customers c 
    ON f.customer_id = c.customer_id
INNER JOIN dim_location o 
    ON f.branch_id = o.branch_id
