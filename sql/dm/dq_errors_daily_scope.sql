SELECT DISTINCT toDate(_load_ts) AS load_date
FROM ods.event_errors_dist
UNION DISTINCT
SELECT DISTINCT toDate(_load_ts) AS load_date
FROM ods.order_errors_dist