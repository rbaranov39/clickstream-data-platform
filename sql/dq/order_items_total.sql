WITH
actual_side AS
(
    SELECT
        order_date,
        order_id,
        sum(price * quantity) AS actual_total,
        1 AS actual_marker
    FROM dds.order_v
    LEFT ARRAY JOIN
        item_price AS price,
        item_quantity AS quantity
    GROUP BY
        order_date,
        order_id
),
reference_side AS
(
    SELECT
        order_date,
        order_id,
        items_total AS reference_total,
        1 AS reference_marker
    FROM dds.order_v
),
comparison AS
(
    SELECT
        coalesce(reference_side.order_date, actual_side.order_date) AS data_date,
        coalesce(reference_side.order_id, actual_side.order_id) AS order_id,
        reference_total,
        actual_total,
        isNotNull(reference_marker) AS reference_present,
        isNotNull(actual_marker) AS actual_present,
        isNull(reference_marker)
            OR isNull(actual_marker)
            OR NOT isNotDistinctFrom(reference_total, actual_total) AS failed
    FROM reference_side
    GLOBAL FULL OUTER JOIN actual_side USING (order_date, order_id)
)
SELECT
    data_date,
    countIf(reference_present) AS reference_rows,
    countIf(actual_present) AS actual_rows,
    countIf(failed) AS failed_rows,
    groupArrayIf(20)(
        concat(
            'order_id=', order_id,
            ', expected=', coalesce(toString(reference_total), 'нет ключа'),
            ', actual=', coalesce(toString(actual_total), 'нет ключа')
        ),
        failed
    ) AS diagnostics
FROM comparison
GROUP BY data_date
ORDER BY data_date

SETTINGS
    join_use_nulls = 1