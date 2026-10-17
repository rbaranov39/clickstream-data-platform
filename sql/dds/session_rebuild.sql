ALTER TABLE dds.session_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dds.session_stage_dist (
    session_id,
    client_id,
    session_date,
    started_at,
    finished_at,
    duration_seconds,
    events_total,
    pageviews,
    cart_adds,
    purchases,
    entry_url,
    exit_url,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    device_category,
    region_city,
    _load_id,
    _load_ts
)
WITH
t1 AS
(SELECT
	*,
	lag(event_time) OVER(PARTITION BY client_id ORDER BY event_time, event_id) AS prew_event_time,
	lag(event_date) OVER(PARTITION BY client_id ORDER BY event_time, event_id) AS prew_event_date,
	CASE
		WHEN prew_event_time = '1970-01-01 00:00:00'
			OR  event_time - prew_event_time > 1800
			OR prew_event_date != event_date THEN 1
		ELSE 0
	END AS start_of_new_session
FROM dds.event_v AS ev),
t2 AS
(SELECT
	*,
	sum(start_of_new_session) OVER(PARTITION BY client_id ORDER BY event_time, event_id) AS number_of_session
FROM t1)
SELECT
    cityHash64(client_id, min(event_time)) AS session_id,
	client_id,
	event_date,
	min(event_time) AS started_at,
	max(event_time) AS finished_at,
	max(event_time) - min(event_time) AS duration_seconds,
	count() AS events_total,
	countIf(event_type = 'pageview') AS pageviews,
	countIf(event_type = 'add_to_cart') AS cart_adds,
	countIf(event_type = 'purchase') AS purchases,
	argMin(url, tuple(event_time, event_id)) AS entry_url,
	argMax(url, tuple(event_time, event_id)) AS exit_url,
	argMin(utm_source, tuple(event_time, event_id)) AS utm_source,
	argMin(utm_medium, tuple(event_time, event_id)) AS utm_medium,
	argMin(utm_campaign, tuple(event_time, event_id)) AS utm_campaign,
	argMin(utm_content, tuple(event_time, event_id)) AS utm_content,
	argMin(utm_term, tuple(event_time, event_id)) AS utm_term,
	argMin(device_category, tuple(event_time, event_id)) AS device_category,
	argMin(region_city, tuple(event_time, event_id)) AS region_city,
	{load_id:String} AS _load_id,
	now64(3, 'UTC') AS _load_ts
FROM t2
WHERE event_date >= {first_day:Date} AND event_date <= {last_day:Date}
GROUP BY client_id, event_date, number_of_session
SETTINGS
    distributed_foreground_insert = 1;