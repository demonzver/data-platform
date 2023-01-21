INSERT INTO default.rate_pairs (`date`, dt_add, rate1_rate2, base, rate1, rate2)
SELECT
	date
	,dt_add
	,concat(JSONExtractKeysAndValuesRaw(rates_key_value)[1].1, JSONExtractKeysAndValuesRaw(rates_key_value)[2].1) as rate1_rate2
	,base
	,JSONExtractKeysAndValuesRaw(rates_key_value)[1].2 as rate1
	,JSONExtractKeysAndValuesRaw(rates_key_value)[2].2 as rate2
FROM (
	SELECT
		dt_add
		,replaceRegexpAll(visitParamExtractRaw(json_string, 'base'), '"', '') as base
		,arrayJoin(JSONExtractKeysAndValuesRaw(visitParamExtractRaw(json_string, 'rates'))).1 as date
		,arrayJoin(JSONExtractKeysAndValuesRaw(visitParamExtractRaw(json_string, 'rates'))).2 as rates_key_value
	FROM json_raw
	WHERE dt_add = (SELECT argMax(dt_add, dt_add) FROM `default`.json_raw)
)