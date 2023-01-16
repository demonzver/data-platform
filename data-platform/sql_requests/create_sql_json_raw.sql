CREATE TABLE IF NOT EXISTS `default`.json_raw
(`dt` DateTime,
`json_string` String)
ENGINE = MergeTree
PARTITION BY toYYYYMM(`dt`)
ORDER BY dt