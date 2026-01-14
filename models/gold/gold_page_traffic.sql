WITH base AS (
    SELECT *
    FROM {{ ref('silver_logs') }}
)
 
SELECT
    url_path,
    COUNT(*) AS total_requests,
    COUNT(DISTINCT ip_address) AS unique_visitors
FROM base
GROUP BY url_path
ORDER BY total_requests DESC