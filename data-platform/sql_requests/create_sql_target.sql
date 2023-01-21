CREATE TABLE IF NOT EXISTS {{ params.rate_pairs }}
(`
    date` DateTime,
    `dt_add` DateTime,
    `rate1_rate2` Nullable(String),
    `base` Nullable(String),
    `rate1` Nullable(Float64),
    `rate2` Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(`date`)
ORDER BY `date`