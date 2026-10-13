ALTER TABLE dm.session_overview_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dm.session_overview_stage_dist (
    report_date,
	device_category,
	sessions,
	visitors,
	bounce_sessions,
	purchase_sessions,
	duration_seconds_total,
	events_total,
	pageviews,
	cart_adds,
	purchases,
	_load_id,
	_load_ts
)
SELECT
    sv.session_date AS report_date,
	sv.device_category,
	count(sv.session_id) AS sessions,
	count(DISTINCT sv.client_id) AS visitors,
	countIf(sv.events_total = 1) AS bounce_sessions,
	countIf(sv.purchases >= 1) AS purchase_sessions,
	sum(sv.duration_seconds) AS duration_seconds_total,
	sum(sv.events_total) AS events_total,
	sum(sv.pageviews) AS pageviews,
	sum(sv.cart_adds) AS cart_adds,
	sum(sv.purchases) AS purchases,
	{load_id:String} AS _load_id,
	now64(3, 'UTC') AS _load_ts
FROM
    dds.session_v AS sv
WHERE session_date >= {first_day:Date} AND session_date <= {last_day:Date}
GROUP BY report_date, device_category

SETTINGS
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;