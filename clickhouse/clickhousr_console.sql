show tables;

select *
from
    person;

CREATE TABLE UAct
(
    UserID    UInt64,
    PageViews UInt8,
    Duration  UInt8,
    Sign      Int8
)
    ENGINE = CollapsingMergeTree(Sign)
        ORDER BY UserID;

INSERT INTO UAct
VALUES (4324182021466249494, 5, 146, 1);
INSERT INTO UAct
VALUES (4324182021466249494, 5, 146, -1),
       (4324182021466249494, 6, 185, 1);

SELECT *
FROM
    UAct;

SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign)  AS Duration
FROM
    UAct
GROUP BY
    UserID
HAVING
    sum(Sign) > 0;

SELECT *
FROM
    UAct FINAL;
