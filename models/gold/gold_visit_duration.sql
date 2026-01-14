WITH base AS (
    SELECT *
    FROM {{ ref('silver_logs') }}
),
visit_duration AS (
    SELECT
        ip_address,
        MAX(log_date) - MIN(log_date) AS visit_duration
    FROM base
    GROUP BY ip_address
)
 
SELECT
    AVG(EXTRACT(EPOCH FROM visit_duration)) AS avg_duration_seconds
FROM visit_duration