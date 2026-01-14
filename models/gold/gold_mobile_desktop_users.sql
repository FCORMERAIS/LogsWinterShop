WITH base AS (

    SELECT *

    FROM {{ ref('silver_logs') }}

)
 
SELECT

    COUNT(*) FILTER (WHERE user_agent ILIKE '%mobile%') AS mobile_visitors,

    COUNT(*) FILTER (WHERE user_agent NOT ILIKE '%mobile%') AS desktop_visitors

FROM base

 