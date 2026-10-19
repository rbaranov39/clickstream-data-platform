WITH
reference AS
(SELECT 
	toDate(created_at, 'Europe/Samara') AS data_date,
	count(DISTINCT order_id) AS orders_cnt,
	max(toTimeZone(updated_at, 'Europe/Samara')) AS max_update
FROM ods.order_v
GROUP BY data_date),
actual AS
(SELECT
	order_date AS data_date,
	count(DISTINCT order_id) AS orders_cnt,
	max(updated_at) AS max_update
FROM dds.order_v
GROUP BY data_date)
SELECT
	coalesce(reference.data_date, actual.data_date) AS data_date,
	COALESCE(reference.orders_cnt, 0) AS reference_rows,
	COALESCE(actual.orders_cnt, 0) AS actual_rows,
	if(
		isNull(reference.data_date)
		OR isNull(actual.data_date)
		OR NOT isNotDistinctFrom(reference.orders_cnt, actual.orders_cnt)
		OR NOT isNotDistinctFrom(reference.max_update, actual.max_update),
		1, 0
	) AS failed_rows,
	if(
    failed_rows = 1,
    [
        concat(
            toString(data_date),
            ': ODS=(orders=', toString(reference_rows),
            ', max_updated=',
            coalesce(toString(reference.max_update), 'нет дня'),
            '), DDS=(orders=', toString(actual_rows),
            ', max_updated=',
            coalesce(toString(actual.max_update), 'нет дня'),
            ')'
        )
    ],
    CAST([], 'Array(String)')
) AS diagnostics
FROM reference GLOBAL FULL OUTER JOIN actual USING(data_date)

SETTINGS
    join_use_nulls = 1









