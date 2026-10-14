ALTER TABLE dm.dq_errors_daily_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dm.dq_errors_daily_stage_dist (
    load_date,
    source,
    error_class,
    error_rows,
    _load_id,
    _load_ts
)
SELECT
	toDate(eed._load_ts) AS load_date,
	'event' AS source,
	error_class,
	count() AS error_rows,
	{load_id:String} AS _load_id,
	now64(3, 'UTC') AS _load_ts
FROM ods.event_errors_dist AS eed
GROUP BY load_date, source, error_class

UNION ALL

SELECT
	toDate(oed._load_ts) AS load_date,
	'order' AS source,
	error_class,
	count() AS error_rows,
	{load_id:String} AS _load_id,
	now64(3, 'UTC') AS _load_ts
FROM ods.order_errors_dist AS oed
GROUP BY load_date, source, error_class

SETTINGS
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;