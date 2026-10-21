WITH
(SELECT
	max(snapshot_date)
FROM ods.order_v) AS max_snapshot_day,
(SELECT
	min(toDate(toTimeZone(created_at, 'Europe/Samara')))
FROM ods.order_v
WHERE snapshot_date = max_snapshot_day) AS first_day_in_snapshot,
(SELECT
	max(toDate(toTimeZone(created_at, 'Europe/Samara')))
FROM ods.order_v
WHERE snapshot_date = max_snapshot_day) AS last_day_in_snapshot,
(SELECT
	minOrNull(toDate(toTimeZone(created_at, 'Europe/Samara')))
FROM ods.order_v
WHERE toDate(toTimeZone(created_at, 'Europe/Samara')) GLOBAL NOT IN (SELECT order_date FROM dds.order_v)) AS first_missing_day
SELECT
	 if(
        first_missing_day IS NULL,
        first_day_in_snapshot,
        least(first_day_in_snapshot, first_missing_day)
	 ) AS first_day,
	last_day_in_snapshot AS last_day
WHERE (SELECT count() FROM ods.order_v) > 0