WITH base AS (
    SELECT *
    FROM {{ ref('silver_logs') }}
)

SELECT
    url_path,
    COUNT(*) FILTER (WHERE status_code >= 400) AS error_count,
    COUNT(*) FILTER (WHERE status_code < 400)  AS success_count
FROM base
GROUP BY url_path
ORDER BY error_count DESC
