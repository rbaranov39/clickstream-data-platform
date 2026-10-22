--- 6-1

WITH
t1 AS
(SELECT
	*
FROM stg.hits_raw_dist
WHERE toStartOfMinute(_load_ts) = '2026-10-22 10:52:00')
SELECT
	kafka_topic, 
	kafka_partition,
	max(kafka_offset),
	min(kafka_offset)
FROM t1
GROUP BY kafka_topic, kafka_partition
;

-- Координаты зафиксированы правильно:
-- partition 0: offsets 195482–195700 → 219 строк;
-- partition 1: offsets 258097–258336 → 240 строк;
-- всего: 219 + 240 = 459.

DESCRIBE TABLE ods.event_rep;


CREATE TABLE IF NOT EXISTS default.test
(
	WatchID	UInt64,
	VisitID	UInt64,
	ClientID UInt64,
	CounterID UInt32,
	EventDate Date,
	UTCEventTime DateTime('UTC'),
	ClientTimeZone Int16,
	EventType LowCardinality(String),
	Sign Int8,
	URL String,
	Referer String,
	Title String,
	UTMSource String,
	UTMMedium String,
	UTMCampaign	String,
	UTMContent String,
	UTMTerm	String,
	LastTrafficSource String,
	HasGCLID UInt8,
	YCLID UInt64,
	Browser String,
	BrowserMajorVersion UInt16,
	BrowserLanguage String,
	OperatingSystem String,
	OperatingSystemRoot String,
	DeviceCategory UInt8,
	MobilePhoneModel String,
	ScreenWidth UInt16,
	ScreenHeight UInt16,
	IPAddress String,
	RegionCountry String,
	RegionCity String,
	RegionCountryID UInt32,
	RegionCityID UInt32,
	GoalsReached Array(UInt32),
	ParsedParamsKey1 Array(String),
	purchaseID Array(String),
	purchaseRevenue Array(Decimal(18, 2)),
	purchaseCurrency Array(String),
	purchaseCoupon Array(String),
	productID Array(String),
	productName Array(String),
	productCategory Array(String),
	productPrice Array(Int64),
	productQuantity Array(UInt64),
	productEventType Array(String),
	ecommerce String,
	_load_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(_load_ts)
PARTITION BY EventDate
ORDER BY (CounterID, EventDate, intHash32(ClientID), WatchID);

INSERT INTO default.test(
	WatchID,
	VisitID,
	ClientID,
	CounterID,
	EventDate,
	UTCEventTime,
	ClientTimeZone,
	EventType,
	Sign,
	URL,
	Referer,
	Title,
	UTMSource,
	UTMMedium,
	UTMCampaign,
	UTMContent,
	UTMTerm,
	LastTrafficSource,
	HasGCLID,
	YCLID,
	Browser,
	BrowserMajorVersion,
	BrowserLanguage,
	OperatingSystem,
	OperatingSystemRoot,
	DeviceCategory,
	MobilePhoneModel,
	ScreenWidth,
	ScreenHeight,
	IPAddress,
	RegionCountry,
	RegionCity,
	RegionCountryID,
	RegionCityID,
	GoalsReached,
	ParsedParamsKey1,
	purchaseID,
	purchaseRevenue,
	purchaseCurrency,
	purchaseCoupon,
	productID,
	productName,
	productCategory,
	productPrice,
	productQuantity,
	productEventType,
	ecommerce,
	_load_ts
)
WITH
source AS
(SELECT *
FROM stg.hits_raw_dist
WHERE _load_ts >= '2026-10-22 10:52:00' AND _load_ts < '2026-10-22 10:53:00'),
[
	'WatchID',
	'VisitID',
	'ClientID',
	'CounterID',
	'EventDate',
	'UTCEventTime',
	'ClientTimeZone',
	'EventType',
	'Sign',
	'URL',
	'Referer',
	'Title',
	'UTMSource',
	'UTMMedium',
	'UTMCampaign', 
	'UTMContent',
	'UTMTerm',
	'LastTrafficSource',
	'HasGCLID',
	'YCLID',
	'Browser',
	'BrowserMajorVersion',
	'BrowserLanguage',
	'OperatingSystem',
	'OperatingSystemRoot',
	'DeviceCategory',
	'MobilePhoneModel',
	'ScreenWidth',
	'ScreenHeight',
	'IPAddress',
	'RegionCountry',
	'RegionCity',
	'RegionCountryID',
	'RegionCityID',
	'GoalsReached',
	'ParsedParamsKey1',
	'purchaseID',
	'purchaseRevenue',
	'purchaseCurrency',
	'purchaseCoupon', 
	'productID',
	'productName',
	'productCategory',
	'productPrice',
	'productQuantity',
	'productEventType',
	'ecommerce'
] AS expected_keys,
checked AS
(SELECT
	*,
	if(JSONType(raw) = 'Object', 1, 0) AS is_object,
	if(arraySort(JSONExtractKeys(raw)) = arraySort(expected_keys), 1, 0) AS keys_match,
	if(
		JSONExtract(raw, 'WatchID', 'Nullable(UInt64)') IS NOT NULL
		AND JSONExtract(raw, 'VisitID', 'Nullable(UInt64)') IS NOT NULL
		AND JSONExtract(raw, 'ClientID', 'Nullable(UInt64)') IS NOT NULL
		AND JSONExtract(raw, 'EventDate', 'Nullable(Date)') IS NOT NULL
		AND parseDateTimeOrNull(
    		JSONExtractString(raw, 'UTCEventTime'), '%Y-%m-%dT%H:%i:%SZ', 'UTC'
		) IS NOT NULL
		, 1, 0
	) AS fields_valid
FROM source),
validated AS
(SELECT
	*,
	if(is_object AND keys_match AND fields_valid, 1, 0) AS row_is_valid
FROM checked)
SELECT
	JSONExtract(raw, 'WatchID',	'UInt64') AS WatchID,
	JSONExtract(raw, 'VisitID',	'UInt64') AS VisitID,
	JSONExtract(raw, 'ClientID', 'UInt64') AS ClientID,
	JSONExtract(raw, 'CounterID', 'UInt32') AS CounterID,
	JSONExtract(raw, 'EventDate', 'Date') AS EventDate,
	assumeNotNull(parseDateTimeOrNull(
		JSONExtractString(raw, 'UTCEventTime'), '%Y-%m-%dT%H:%i:%SZ', 'UTC'
	)) AS UTCEventTime,
	JSONExtract(raw, 'ClientTimeZone', 'Int16') AS ClientTimeZone,
	JSONExtractString(raw, 'EventType') AS EventType,
	JSONExtract(raw, 'Sign', 'Int8') AS Sign,
	JSONExtractString(raw, 'URL') AS URL,
	JSONExtractString(raw, 'Referer') AS Referer,
	JSONExtractString(raw, 'Title') AS Title,
	JSONExtractString(raw, 'UTMSource') AS UTMSource,
	JSONExtractString(raw, 'UTMMedium') AS UTMMedium,
	JSONExtractString(raw, 'UTMCampaign') AS UTMCampaign,
	JSONExtractString(raw, 'UTMContent') AS UTMContent,
	JSONExtractString(raw, 'UTMTerm') AS UTMTerm,
	JSONExtractString(raw, 'LastTrafficSource') AS LastTrafficSource,
	JSONExtract(raw, 'HasGCLID', 'UInt8') AS HasGCLID,
	JSONExtract(raw, 'YCLID', 'UInt64') AS YCLID,
	JSONExtractString(raw, 'Browser') AS Browser,
	JSONExtract(raw, 'BrowserMajorVersion', 'UInt16') AS BrowserMajorVersion,
	JSONExtractString(raw, 'BrowserLanguage') AS BrowserLanguage,
	JSONExtractString(raw, 'OperatingSystem') AS OperatingSystem,
	JSONExtractString(raw, 'OperatingSystemRoot') AS OperatingSystemRoot,
	JSONExtract(raw, 'DeviceCategory', 'UInt8') AS DeviceCategory,
	JSONExtractString(raw, 'MobilePhoneModel') AS MobilePhoneModel,
	JSONExtract(raw, 'ScreenWidth', 'UInt16') AS ScreenWidth,
	JSONExtract(raw, 'ScreenHeight', 'UInt16') AS ScreenHeight,
	JSONExtractString(raw, 'IPAddress') AS IPAddress,
	JSONExtractString(raw, 'RegionCountry') AS RegionCountry,
	JSONExtractString(raw, 'RegionCity') AS RegionCity,
	JSONExtract(raw, 'RegionCountryID', 'UInt32') AS RegionCountryID,
	JSONExtract(raw, 'RegionCityID', 'UInt32') AS RegionCityID,
	JSONExtract(raw, 'GoalsReached', 'Array(UInt32)') AS GoalsReached,
	JSONExtract(raw, 'ParsedParamsKey1', 'Array(String)') AS ParsedParamsKey1,
	JSONExtract(raw, 'purchaseID', 'Array(String)') AS purchaseID,
	JSONExtract(raw, 'purchaseRevenue', 'Array(Decimal(18, 2))') AS purchaseRevenue,
	JSONExtract(raw, 'purchaseCurrency', 'Array(String)') AS purchaseCurrency,
	JSONExtract(raw, 'purchaseCoupon', 'Array(String)') AS purchaseCoupon,
	JSONExtract(raw, 'productID', 'Array(String)') AS productID,
	JSONExtract(raw, 'productName', 'Array(String)') AS productName,
	JSONExtract(raw, 'productCategory', 'Array(String)') AS productCategory,
	JSONExtract(raw, 'productPrice', 'Array(Int64)') AS productPrice,
	JSONExtract(raw, 'productQuantity', 'Array(UInt64)') AS productQuantity,
	JSONExtract(raw, 'productEventType', 'Array(String)') AS productEventType,
	JSONExtractString(raw, 'ecommerce') AS ecommerce,
	_load_ts
FROM validated
WHERE row_is_valid = 1;

SELECT * FROM default.test;

SELECT * FROM stg.hits_raw_dist;

SELECT count() FROM ods.event_v WHERE _load_ts >= '2026-10-22 10:52:00' AND _load_ts < '2026-10-22 10:53:00';


WITH
t1 AS
(SELECT *, intHash32(ClientID) AS client_hash, 1 AS def_marker FROM default.test),
t2 AS
(SELECT *, intHash32(ClientID) AS client_hash, 1 AS ods_marker FROM ods.event_v WHERE _load_ts >= '2026-10-22 10:52:00' AND _load_ts < '2026-10-22 10:53:00'),
t3 AS
(SELECT def_marker, ods_marker FROM t1 FULL OUTER JOIN t2 USING(CounterID, EventDate, client_hash, WatchID))
SELECT
	countIf(def_marker > ods_marker) AS miss_in_ods,
	countIf(def_marker < ods_marker) AS miss_in_def
FROM t3


WITH
    t1 AS
    (
        SELECT
            *,
            intHash32(ClientID) AS client_hash,
            1 AS test_marker
        FROM default.test
    ),
    t2 AS
    (
        SELECT
            *,
            intHash32(ClientID) AS client_hash,
            1 AS ods_marker
        FROM ods.event_v
        WHERE _load_ts >= '2026-10-22 10:52:00' AND _load_ts < '2026-10-22 10:53:00'
    )
SELECT
    countIf(isNull(test_marker)) AS missing_in_test,
    countIf(isNull(ods_marker)) AS missing_in_ods,
    countIf(
        test_marker = 1
        AND ods_marker = 1
        AND (
            t1.VisitID != t2.VisitID
            OR t1.ClientID != t2.ClientID
            OR t1.UTCEventTime != t2.UTCEventTime
            OR t1.ClientTimeZone != t2.ClientTimeZone
            OR t1.EventType != t2.EventType
            OR t1.Sign != t2.Sign
            OR t1.URL != t2.URL
            OR t1.Referer != t2.Referer
            OR t1.Title != t2.Title
            OR t1.UTMSource != t2.UTMSource
            OR t1.UTMMedium != t2.UTMMedium
            OR t1.UTMCampaign != t2.UTMCampaign
            OR t1.UTMContent != t2.UTMContent
            OR t1.UTMTerm != t2.UTMTerm
            OR t1.LastTrafficSource != t2.LastTrafficSource
            OR t1.HasGCLID != t2.HasGCLID
            OR t1.YCLID != t2.YCLID
            OR t1.Browser != t2.Browser
            OR t1.BrowserMajorVersion != t2.BrowserMajorVersion
            OR t1.BrowserLanguage != t2.BrowserLanguage
            OR t1.OperatingSystem != t2.OperatingSystem
            OR t1.OperatingSystemRoot != t2.OperatingSystemRoot
            OR t1.DeviceCategory != t2.DeviceCategory
            OR t1.MobilePhoneModel != t2.MobilePhoneModel
            OR t1.ScreenWidth != t2.ScreenWidth
            OR t1.ScreenHeight != t2.ScreenHeight
            OR t1.IPAddress != t2.IPAddress
            OR t1.RegionCountry != t2.RegionCountry
            OR t1.RegionCity != t2.RegionCity
            OR t1.RegionCountryID != t2.RegionCountryID
            OR t1.RegionCityID != t2.RegionCityID
            OR t1.GoalsReached != t2.GoalsReached
            OR t1.ParsedParamsKey1 != t2.ParsedParamsKey1
            OR t1.purchaseID != t2.purchaseID
            OR t1.purchaseRevenue != t2.purchaseRevenue
            OR t1.purchaseCurrency != t2.purchaseCurrency
            OR t1.purchaseCoupon != t2.purchaseCoupon
            OR t1.productID != t2.productID
            OR t1.productName != t2.productName
            OR t1.productCategory != t2.productCategory
            OR t1.productPrice != t2.productPrice
            OR t1.productQuantity != t2.productQuantity
            OR t1.productEventType != t2.productEventType
            OR t1.ecommerce != t2.ecommerce
        )
    ) AS field_mismatches
FROM t1
FULL OUTER JOIN t2
    USING (CounterID, EventDate, client_hash, WatchID)
SETTINGS join_use_nulls = 1;
