CREATE VIEW dm.events_enriched_v
ON CLUSTER clickstream_cluster AS
SELECT
	event_date,
	event_time,
	toHour(event_time) AS event_hour,
	client_id,
	event_id,
	event_type,
	url,
	concat('/', splitByChar('?', arrayStringConcat(arraySlice(splitByChar('/', splitByString('://', url)[2]), 2), '/'))[1]) AS page_path,
	referer,
	splitByChar('/', splitByString('://', referer)[2])[1] AS referer_host,
	title,
	utm_source,
	utm_medium,
	utm_campaign,
	utm_content,
	utm_term,
	device_category,
	browser,
	operating_system,
	region_country,
	region_city,
	product_id,
	product_name,
	product_category,
	product_price,
	product_quantity,
	product_event_type,
	purchase_id,
	purchase_revenue,
	purchase_coupon
FROM dds.event_v;

CREATE TABLE IF NOT EXISTS dm.top_pages_daily_rep
ON CLUSTER clickstream_cluster
(
	report_date Date,
	page_path LowCardinality(String),
	pageviews UInt64,
	visitors UInt64,
	cart_adds UInt64,
	purchases UInt64,
	_load_id String,
	_load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}',
    '{replica}'
)
PARTITION BY report_date
ORDER BY (report_date, page_path);

CREATE TABLE IF NOT EXISTS dm.top_pages_daily_dist
ON CLUSTER clickstream_cluster
AS dm.top_pages_daily_rep
ENGINE = Distributed(
	'clickstream_cluster',
	'dm',
	'top_pages_daily_rep',
	cityHash64(page_path)
);

CREATE TABLE IF NOT EXISTS dm.top_pages_daily_stage_rep
ON CLUSTER clickstream_cluster
AS dm.top_pages_daily_rep
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/{database}/{table}',
    '{replica}'
)
PARTITION BY report_date
ORDER BY (report_date, page_path);

CREATE TABLE IF NOT EXISTS dm.top_pages_daily_stage_dist
ON CLUSTER clickstream_cluster
AS dm.top_pages_daily_stage_rep
ENGINE = Distributed(
	'clickstream_cluster',
	'dm',
	'top_pages_daily_stage_rep',
	cityHash64(page_path)
);

CREATE VIEW IF NOT EXISTS dm.top_pages_daily_v
ON CLUSTER clickstream_cluster AS
SELECT
	report_date,
	page_path,
	pageviews,
	visitors,
	cart_adds,
	purchases,
	_load_id,
	_load_ts
FROM dm.top_pages_daily_dist;
