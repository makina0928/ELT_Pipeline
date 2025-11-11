with locations as (
    select * 
    from {{ source('portfolio_data', 'location') }}
)
SELECT
    CAST(branch_id AS VARCHAR)        AS branch_id,
    CAST(branch_name AS VARCHAR)      AS branch_name,
    CAST(region AS VARCHAR)           AS region,
    CAST(country AS VARCHAR)          AS country,
    CAST(manager_name AS VARCHAR)     AS manager_name,
    
    CASE
        WHEN opened_date::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN opened_date::date
        ELSE NULL
    END AS opened_date,

    CAST(branch_type AS VARCHAR)      AS branch_type
FROM locations
