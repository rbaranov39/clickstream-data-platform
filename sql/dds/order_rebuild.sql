ALTER TABLE dds.order_stage_rep ON CLUSTER clickstream_cluster
DROP PARTITION ALL;

INSERT INTO dds.order_stage_dist(
	order_id,
	user_id,
	status,
	order_date,
	created_at,
	updated_at,
	items_total,
	discount,
	delivery,
	total,
	item_sku,
	item_quantity,
	item_price,
	_load_id,
	_load_ts
)
WITH JSONExtract(
	items,
	'Array(Tuple(sku String, qty UInt32, price Decimal(18,2)))'
) AS lines
SELECT
	order_id,
	user_id,
	status,
	toDate(toTimeZone(created_at, 'Europe/Samara')) AS order_date,
	toTimeZone(created_at, 'Europe/Samara') AS created_at,
	toTimeZone(updated_at, 'Europe/Samara')	AS updated_at,
	items_total,
	discount,
	delivery,
	total,
	arrayMap(line -> line.sku, lines) AS item_sku,
	arrayMap(line -> line.qty, lines) AS item_quantity,
	arrayMap(line -> line.price, lines) AS item_price,
	{load_id:String} AS _load_id,
	now64(3, 'UTC') AS _load_ts
FROM ods.order_v
WHERE toDate(toTimeZone(created_at, 'Europe/Samara')) <= {last_day:Date} AND toDate(toTimeZone(created_at, 'Europe/Samara')) >= {first_day:Date}

SETTINGS
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;