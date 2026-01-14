WITH base AS (
    SELECT *
    FROM {{ ref('silver_logs') }}
)
 
SELECT
    referrer,
    COUNT(*) AS total_requests,
    SUM(response_size) AS total_bytes,
    COUNT(*) FILTER (WHERE referrer ILIKE '%google.%') AS google_requests
FROM base
GROUP BY referrer
ORDER BY total_requests DESC