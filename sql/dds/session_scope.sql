-- Вход: {position:UInt16} — номер первого несыгранного модельного дня.
-- Результат: first_day и last_day; при пустом ODS строк нет.
-- Точку с запятой в конце запроса не ставить.
WITH
(SELECT minOrNull(EventDate) FROM ods.event_dist) AS first_living_day,
first_living_day + ({position:UInt16} - 1) AS last_living_day,
(SELECT minOrNull(EventDate) FROM ods.event_dist WHERE EventDate GLOBAL NOT IN (SELECT session_date FROM dds.session_dist)) AS first_missing_day
SELECT
	if(last_living_day >= first_missing_day, first_missing_day, Null) AS first_day,
	last_living_day AS last_day
WHERE (SELECT count() FROM ods.event_dist) > 0