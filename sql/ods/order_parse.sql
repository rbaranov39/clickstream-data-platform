INSERT INTO ods.order_dist(
	order_id,
	user_id,
	status,
	created_at,
	updated_at,
	items_total,
	discount,
	delivery,
	total,
	items,
	snapshot_date,
	_load_id,
	_load_ts
)
WITH
t1 AS
(SELECT
	*,
	if(JSONType(raw) = 'Object', 1, 0)	AS is_object,
	if(arraySort([
        'order_id',
        'user_id',
        'status',
        'created_at',
        'updated_at',
        'items_total',
        'discount',
        'delivery',
        'total',
        'items',
        'snapshot_date'
    ]) = arraySort(JSONExtractKeys(raw)), 1, 0) AS keys_match,
    if(
        JSONType(raw, 'order_id') = 'String'
        AND JSONType(raw, 'user_id') IN ('UInt64', 'Int64')
        AND JSONExtract(raw, 'user_id', 'Nullable(UInt64)') IS NOT NULL
        AND JSONType(raw, 'status') = 'String'
        AND JSONType(raw, 'created_at') = 'String'
        AND match(
            JSONExtractString(raw, 'created_at'), '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$'
		)
		AND parseDateTime64InJodaSyntaxOrNull(
			JSONExtractString(raw, 'created_at'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', 'UTC'
		) IS NOT NULL
        AND JSONType(raw, 'updated_at') = 'String'
		AND match(
            JSONExtractString(raw, 'updated_at'), '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$'
		)
		AND parseDateTime64InJodaSyntaxOrNull(
			JSONExtractString(raw, 'updated_at'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', 'UTC'
		) IS NOT NULL
		AND JSONType(raw, 'items_total') = 'String'
		AND match(
			JSONExtractString(raw, 'items_total'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'items_total'), 2
		) IS NOT NULL
		AND JSONType(raw, 'discount') = 'String'
		AND match(
			JSONExtractString(raw, 'discount'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'discount'), 2
		) IS NOT NULL
		AND JSONType(raw, 'delivery') = 'String'
		AND match(
			JSONExtractString(raw, 'delivery'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'delivery'), 2
		) IS NOT NULL
		AND JSONType(raw, 'total') = 'String'
		AND match(
			JSONExtractString(raw, 'total'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'total'), 2
		) IS NOT NULL
		AND JSONType(raw, 'items') = 'Array'
		AND JSONType(raw, 'snapshot_date') = 'String'
		AND match(
            JSONExtractString(raw, 'snapshot_date'), '^\\d{4}-\\d{2}-\\d{2}$'
		)
		AND parseDateTimeInJodaSyntaxOrNull(
            JSONExtractString(raw, 'snapshot_date'), 'yyyy-MM-dd', 'UTC'
		) IS NOT NULL,
		1, 0
    ) AS fields_valid
FROM stg.orders_raw_dist
WHERE _load_id = {load_id:String}),
t2 AS
(SELECT
	*,
	if(
		is_object AND keys_match AND fields_valid, 1, 0
	) AS row_is_valid
FROM t1)
SELECT
	JSONExtractString(raw, 'order_id') AS order_id,
	JSONExtract(raw, 'user_id', 'UInt64') AS user_id,
	JSONExtractString(raw, 'status') AS status,
	assumeNotNull(parseDateTime64InJodaSyntaxOrNull(
        JSONExtractString(raw, 'created_at'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', 'UTC'
	)) AS created_at,
	assumeNotNull(parseDateTime64InJodaSyntaxOrNull(
        JSONExtractString(raw, 'updated_at'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', 'UTC'
	)) AS updated_at,
	assumeNotNull(toDecimal64OrNull(
        JSONExtractString(raw, 'items_total'), 2
	)) AS items_total,
	assumeNotNull(toDecimal64OrNull(
        JSONExtractString(raw, 'discount'), 2
	)) AS discount,
	assumeNotNull(toDecimal64OrNull(
        JSONExtractString(raw, 'delivery'), 2
	)) AS delivery,
	assumeNotNull(toDecimal64OrNull(
        JSONExtractString(raw, 'total'), 2
	)) AS total,
	JSONExtractRaw(raw, 'items') AS items,
	toDate(assumeNotNull(
			parseDateTimeInJodaSyntaxOrNull(
                JSONExtractString(raw, 'snapshot_date'), 'yyyy-MM-dd', 'UTC'
            )
            )
	) AS snapshot_date,
	_load_id,
	_load_ts
FROM t2
WHERE row_is_valid = 1

SETTINGS
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;

INSERT INTO ods.order_errors_dist(
	raw,
	error_class,
	kafka_topic,
	kafka_partition,
	kafka_offset,
	kafka_timestamp,
	consumer_host,
	_load_id,
	_load_ts
)
WITH
t1 AS
(SELECT
	*,
	if(JSONType(raw) = 'Object', 1, 0)	AS is_object,
	if(arraySort([
        'order_id',
        'user_id',
        'status',
        'created_at',
        'updated_at',
        'items_total',
        'discount',
        'delivery',
        'total',
        'items',
        'snapshot_date'
    ]) = arraySort(JSONExtractKeys(raw)), 1, 0) AS keys_match,
    if(
        JSONType(raw, 'order_id') = 'String'
        AND JSONType(raw, 'user_id') IN ('UInt64', 'Int64')
        AND JSONExtract(raw, 'user_id', 'Nullable(UInt64)') IS NOT NULL
        AND JSONType(raw, 'status') = 'String'
        AND JSONType(raw, 'created_at') = 'String'
		AND match(
            JSONExtractString(raw, 'created_at'), '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$'
		)
		AND parseDateTime64InJodaSyntaxOrNull(
			JSONExtractString(raw, 'created_at'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', 'UTC'
		) IS NOT NULL
        AND JSONType(raw, 'updated_at') = 'String'
		AND match(
            JSONExtractString(raw, 'updated_at'), '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$'
		)
		AND parseDateTime64InJodaSyntaxOrNull(
			JSONExtractString(raw, 'updated_at'), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'', 'UTC'
		) IS NOT NULL
		AND JSONType(raw, 'items_total') = 'String'
		AND match(
			JSONExtractString(raw, 'items_total'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'items_total'), 2
		) IS NOT NULL
		AND JSONType(raw, 'discount') = 'String'
		AND match(
			JSONExtractString(raw, 'discount'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'discount'), 2
		) IS NOT NULL
		AND JSONType(raw, 'delivery') = 'String'
		AND match(
			JSONExtractString(raw, 'delivery'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'delivery'), 2
		) IS NOT NULL
		AND JSONType(raw, 'total') = 'String'
		AND match(
			JSONExtractString(raw, 'total'), '^\\d+\\.\\d{2}$'
		)
		AND toDecimal64OrNull(
            JSONExtractString(raw, 'total'), 2
		) IS NOT NULL
		AND JSONType(raw, 'items') = 'Array'
		AND JSONType(raw, 'snapshot_date') = 'String'
		AND match(
            JSONExtractString(raw, 'snapshot_date'), '^\\d{4}-\\d{2}-\\d{2}$'
		)
		AND parseDateTimeInJodaSyntaxOrNull(
            JSONExtractString(raw, 'snapshot_date'), 'yyyy-MM-dd', 'UTC'
		) IS NOT NULL,
		1, 0
    ) AS fields_valid
FROM stg.orders_raw_dist
WHERE _load_id = {load_id:String}),
t2 AS
(SELECT
	*,
	if(
		is_object AND keys_match AND fields_valid, 1, 0
	) AS row_is_valid
FROM t1)
SELECT
	raw,
	multiIf(
		NOT is_object, 'not_an_object',
		NOT keys_match, 'keyset_mismatch',
		'field_invalid'
	) AS error_class,
	kafka_topic,
	kafka_partition,
	kafka_offset,
	kafka_timestamp,
	consumer_host,
	_load_id,
	_load_ts
FROM t2
WHERE row_is_valid = 0

SETTINGS
    distributed_foreground_insert = 1,
    parallel_distributed_insert_select = 0;