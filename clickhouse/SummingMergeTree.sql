CREATE TABLE summtt
(
    key   UInt32,
    value UInt32
)
    ENGINE = SummingMergeTree()
        ORDER BY key;

INSERT INTO summtt
Values (1, 1),
       (1, 2),
       (2, 1);

SELECT
    key,
    sum(value)
FROM
    summtt
GROUP BY
    key;
