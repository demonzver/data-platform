CREATE TABLE IF NOT EXISTS {{ params.json_raw }}
(
    dt_add DateTime,
    json_string String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt_add)
ORDER BY dt_add