WITH base AS (
    SELECT *
    FROM {{ ref('silver_logs') }}
)

SELECT
    date_trunc('hour', log_date) AS hour,
    COUNT(DISTINCT ip_address) AS visitors_count
FROM base
GROUP BY hour
