WITH
    (SELECT 
        minOrNull(event_date)
    FROM dds.event_v
    WHERE event_date GLOBAL NOT IN (
        SELECT report_date
        FROM dm.utm_effectiveness_dist
    )) AS first_day,
    (SELECT
        maxOrNull(event_date)
    FROM dds.event_v) AS last_day
SELECT
    first_day,
    last_day