CREATE TABLE UActCollapsingMergeTree
(
    UserID    UInt64,
    PageViews UInt8,
    Duration  UInt8,
    Sign      Int8
)
    ENGINE = CollapsingMergeTree(Sign)
        ORDER BY UserID;


INSERT INTO UActCollapsingMergeTree
VALUES (4324182021466249494, 5, 146, 1);
INSERT INTO UActCollapsingMergeTree
VALUES (4324182021466249494, 5, 146, -1),
       (4324182021466249494, 6, 185, 1);


SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign)  AS Duration
FROM
    UActCollapsingMergeTree
GROUP BY
    UserID
HAVING
    sum(Sign) > 0;


SELECT *
FROM
    UActCollapsingMergeTree FINAL;

