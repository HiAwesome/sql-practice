CREATE TABLE UActVersionedCollapsingMergeTree
(
    UserID    UInt64,
    PageViews UInt8,
    Duration  UInt8,
    Sign      Int8,
    Version   UInt8
)
    ENGINE = VersionedCollapsingMergeTree(Sign, Version)
        ORDER BY UserID;

INSERT INTO UActVersionedCollapsingMergeTree
VALUES (4324182021466249494, 5, 146, 1, 1);
INSERT INTO UActVersionedCollapsingMergeTree
VALUES (4324182021466249494, 5, 146, -1, 1),
       (4324182021466249494, 6, 185, 1, 2);


SELECT *
FROM
    UActVersionedCollapsingMergeTree;

SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign)  AS Duration,
    Version
FROM
    UActVersionedCollapsingMergeTree
GROUP BY
    UserID,
    Version
HAVING
    sum(Sign) > 0;

SELECT *
FROM
    UActVersionedCollapsingMergeTree FINAL;
