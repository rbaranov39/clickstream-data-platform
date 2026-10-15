ALTER TABLE dm.utm_effectiveness_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dm.utm_effectiveness_stage_dist (
    report_date,
    utm_source,
    utm_medium,
    utm_campaign,
    visitors,
    pageviews,
    add_to_carts,
    purchases,
    declared_revenue,
    _load_id,
    _load_ts
)
SELECT
	event_date AS report_date,
	utm_source,
	utm_medium,
	utm_campaign,
	count(DISTINCT client_id) AS visitors,
	countIf(event_type = 'pageview') AS pageviews,
	countIf(event_type = 'add_to_cart') AS add_to_carts,
	countIf(event_type = 'purchase') AS purchases,
	sumIf(purchase_revenue[1], event_type = 'purchase') AS declared_revenue,
	{load_id:String} AS _load_id,
	now64(3, 'UTC') AS _load_ts
FROM dds.event_v
WHERE event_date >= {first_day:Date} AND event_date <= {last_day:Date}
GROUP BY report_date, utm_source, utm_medium, utm_campaign

SETTINGS
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;