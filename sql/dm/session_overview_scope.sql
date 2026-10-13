WITH
    (SELECT 
        minOrNull(session_date)
    FROM dds.session_v
    WHERE session_date GLOBAL NOT IN (
        SELECT report_date
        FROM dm.session_overview_dist
        )
    ) AS first_day,
    (SELECT
        maxOrNull(session_date)
    FROM dds.session_v) AS last_day
SELECT
    first_day,
    last_day