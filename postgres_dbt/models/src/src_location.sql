with locations as (
    select * 
    from {{ source('portfolio_data', 'location') }}
)
SELECT
    TRIM(UPPER(branch_id)) AS branch_id,
    trim(initcap(branch_name)) AS branch_name,
    trim(initcap(region)) AS region,
    trim(initcap(country)) AS country,
    trim(initcap(manager_name)) AS manager_name,
    
    CASE
        WHEN opened_date::text ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            THEN opened_date::date
        ELSE NULL
    END AS opened_date,

    trim(initcap(branch_type)) AS branch_type
FROM locations
