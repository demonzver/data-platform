INSERT INTO `default`.rate_pairs (`date`, dt, rate1_rate2, base, rate1, rate2)
SELECT
	toDateTime(parseDateTimeBestEffortOrNull(replaceRegexpAll(visitParamExtractRaw(json_string, 'date'), '"', ''))) AS `date`,
	`dt`,
	'BTC_USD' as rate1_rate2,
	replaceRegexpAll(visitParamExtractRaw(json_string, 'base'), '"', '') as base,
    toFloat64OrNull(trim(BOTH ' ' FROM visitParamExtractRaw(visitParamExtractRaw(json_string, 'rates'), 'BTC'))) as rate1,
    toFloat64OrNull(trim(BOTH ' ' FROM visitParamExtractRaw(visitParamExtractRaw(json_string, 'rates'), 'USD'))) as rate2
FROM json_raw
WHERE dt = (SELECT argMax(dt, dt) FROM `default`.json_raw)