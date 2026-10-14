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

CREATE TABLE IF NOT EXISTS dm.session_overview_rep
ON CLUSTER clickstream_cluster
(
	report_date Date,
	device_category LowCardinality(String),
	sessions UInt64,
	visitors UInt64,
	bounce_sessions UInt64,
	purchase_sessions UInt64,
	duration_seconds_total UInt64,
	events_total UInt64,
	pageviews UInt64,
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
ORDER BY (report_date, device_category);

CREATE TABLE IF NOT EXISTS dm.session_overview_dist
ON CLUSTER clickstream_cluster
AS dm.session_overview_rep
ENGINE = Distributed(
	'clickstream_cluster',
	'dm',
	'session_overview_rep',
	cityHash64(report_date, device_category)
);

CREATE TABLE IF NOT EXISTS dm.session_overview_stage_rep
ON CLUSTER clickstream_cluster
AS dm.session_overview_rep
ENGINE = ReplicatedMergeTree(
	'/clickhouse/tables/{shard}/{database}/{table}',
	'{replica}'
)
PARTITION BY report_date
ORDER BY (report_date, device_category);

CREATE TABLE IF NOT EXISTS dm.session_overview_stage_dist
ON CLUSTER clickstream_cluster
AS dm.session_overview_stage_rep
ENGINE = Distributed(
	'clickstream_cluster',
	'dm',
	'session_overview_stage_rep',
	cityHash64(report_date, device_category)
);

CREATE VIEW IF NOT EXISTS dm.session_overview_v
ON CLUSTER clickstream_cluster AS
SELECT
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
FROM dm.session_overview_dist;

CREATE TABLE IF NOT EXISTS dm.dq_errors_daily_rep
ON CLUSTER clickstream_cluster
(
	load_date Date,
	source LowCardinality(String),
	error_class LowCardinality(String),
	error_rows UInt64,
	_load_id String,
	_load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplicatedMergeTree(
	'/clickhouse/tables/{shard}/{database}/{table}',
	'{replica}'
)
PARTITION BY load_date
ORDER BY (load_date, source, error_class);

CREATE TABLE IF NOT EXISTS dm.dq_errors_daily_dist
ON CLUSTER clickstream_cluster
AS dm.dq_errors_daily_rep
ENGINE = Distributed(
	'clickstream_cluster',
	'dm',
	'dq_errors_daily_rep',
	cityHash64(load_date, source, error_class)
);

CREATE TABLE IF NOT EXISTS dm.dq_errors_daily_stage_rep
ON CLUSTER clickstream_cluster
AS dm.dq_errors_daily_rep
ENGINE = ReplicatedMergeTree(
	'/clickhouse/tables/{shard}/{database}/{table}',
	'{replica}'
)
PARTITION BY load_date
ORDER BY (load_date, source, error_class);

CREATE TABLE IF NOT EXISTS dm.dq_errors_daily_stage_dist
ON CLUSTER clickstream_cluster
AS dm.dq_errors_daily_stage_rep
ENGINE = Distributed(
	'clickstream_cluster',
	'dm',
	'dq_errors_daily_stage_rep',
	cityHash64(load_date, source, error_class)
);

CREATE VIEW IF NOT EXISTS dm.dq_errors_daily_v
ON CLUSTER clickstream_cluster AS
SELECT
	load_date,
	source,
	error_class,
	error_rows,
	_load_id,
	_load_ts
FROM dm.dq_errors_daily_dist;