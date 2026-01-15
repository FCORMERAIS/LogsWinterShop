WITH base AS (
    SELECT *
    FROM {{ ref('silver_logs') }}
)

SELECT
    year,month,day,hour, COUNT(DISTINCT ip_address) AS visitors_count
FROM base
GROUP BY year,month,day,hour
