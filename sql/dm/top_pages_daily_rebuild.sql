ALTER TABLE dm.top_pages_daily_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dm.top_pages_daily_stage_dist (
	report_date,
	page_path,
	pageviews,
	visitors,
	cart_adds,
	purchases,
	_load_id,
	_load_ts
)
SELECT
	event_date AS report_date,
	concat('/', splitByChar('?', arrayStringConcat(arraySlice(splitByChar('/', splitByString('://', url)[2]), 2), '/'))[1]) AS page_path,
	countIf(event_type = 'pageview') AS pageviews,
	count(DISTINCT client_id) AS visitors,
	countIf(event_type = 'add_to_cart') AS cart_adds,
	countIf(event_type = 'purchase') AS purchases,
	{load_id:String} AS _load_id,
	now64(3, 'UTC') AS _load_ts
FROM
	dds.event_v
WHERE event_date >= {first_day:Date} AND event_date <= {last_day:Date}
GROUP BY report_date, page_path

SETTINGS
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;