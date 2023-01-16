CREATE TABLE IF NOT EXISTS `default`.rate_pairs
(`date` DateTime,
`dt` DateTime,
`rate1_rate2` Nullable(String),
`base` Nullable(String),
`rate1` Nullable(Float64),
`rate2` Nullable(Float64) )
ENGINE = MergeTree
PARTITION BY toYYYYMM(`date`)
ORDER BY `date`