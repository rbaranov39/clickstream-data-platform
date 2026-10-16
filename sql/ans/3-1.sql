WITH 
t1 AS 
(SELECT
	ev.`ClientID` AS clientId,
	ev.`WatchID` AS watchId,
	ev.`VisitID` AS visitId,
	ev.`EventType` AS eventType,
	ev.`EventDate` AS eventDate,
	ev.`UTCEventTime` AS utcEventTime,
	lag(ev.`UTCEventTime`) OVER(PARTITION BY ev.`ClientID` ORDER BY ev.`UTCEventTime`) AS prevEventTime,
	lag(ev.`EventDate`) OVER(PARTITION BY ev.`ClientID` ORDER BY ev.`UTCEventTime`) AS prevEventDate,
	CASE
		WHEN prevEventTime = '1970-01-01 00:00:00' 
			OR ev.`UTCEventTime` - prevEventTime > 1800 
			OR prevEventDate != ev.`EventDate` THEN 1
		ELSE 0
	END AS sessionStart
FROM ods.event_v ev),
t2 AS
(SELECT
	*,
	sum(sessionStart) OVER (PARTITION BY clientId ORDER BY utcEventTime) AS numberOfSession
FROM t1),
t3 AS
(SELECT 
	clientId,
	numberOfSession, 
	min(eventDate) AS sessionDate
FROM t2
GROUP BY clientId, numberOfSession),
t4 AS
(SELECT 
	clientId,
	sessionDate,
	count() countOfSessions,
	1 AS test_marker
FROM t3
GROUP BY clientId, sessionDate),
t5 AS
(SELECT
	`ClientID` AS clientId,
	`EventDate` AS sessionDate,
	uniqExact(`VisitID`) AS ods_countOfSessions,
	1 AS ods_marker
FROM ods.event_v
GROUP BY `ClientID`, `EventDate`),
t6 AS
(SELECT
	*
FROM t4 FULL OUTER JOIN t5 USING(clientId, sessionDate))
SELECT 
	countIf(test_marker > ods_marker) AS miss_ods,
	countIf(test_marker < ods_marker) AS miss_test,
	countIf(
    test_marker = 1
    AND ods_marker = 1
    AND countOfSessions != ods_countOfSessions
) AS sessions_mismatch
FROM t6
