with locations as (
    select *
    from {{ ref('src_location') }}
    where branch_id is not null  -- Exclude invalid records
      and trim(cast(branch_id as text)) <> ''   -- Exclude empty strings if any
)

select
    -- create a consistent surrogate key
    {{ dbt_utils.generate_surrogate_key(['branch_id']) }} as branch_id_sur,

    -- original fields
    branch_id,
    branch_name,
    region,
    country,
    manager_name,
    opened_date,
    branch_type
from locations
