-- 练习 经典Hive-SQL面试题 https://jiamaoxiang.top/2019/10/15/%E7%BB%8F%E5%85%B8Hive-SQL%E9%9D%A2%E8%AF%95%E9%A2%98/
/*
第一题
需求
我们有如下的用户访问数据
	userId  visitDate   visitCount
	u01     2017/1/21   5
	u02     2017/1/23   6
	u03     2017/1/22   8
	u04     2017/1/20   3
	u01     2017/1/23   6
	u01     2017/2/21   8
	U02     2017/1/23   6
	U01     2017/2/22   4
要求使用SQL统计出每个用户的累积访问次数，如下表所示：
	用户id    月份      小计 累积
	u01       2017-01 11  11
	u01       2017-02 12  23
	u02       2017-01 12  12
	u03       2017-01 8   8
	u04       2017-01 3   3
*/

CREATE TABLE test1
(
    userId     string,
    visitDate  string,
    visitCount INT
);

INSERT INTO TABLE
    test1
VALUES
    ('u01', '2017/1/21', 5),
    ('u02', '2017/1/23', 6),
    ('u03', '2017/1/22', 8),
    ('u04', '2017/1/20', 3),
    ('u01', '2017/1/23', 6),
    ('u01', '2017/2/21', 8),
    ('u02', '2017/1/23', 6),
    ('u01', '2017/2/22', 4);

SELECT
    t2.userid,
    t2.visitmonth,
    subtotal_visit_cnt,
    sum(subtotal_visit_cnt) over (partition BY userid ORDER BY visitmonth) AS total_visit_cnt
FROM
    (SELECT
         userid,
         visitmonth,
         sum(visitcount) AS subtotal_visit_cnt
     FROM
         (SELECT
              userid,
              date_format(regexp_replace(visitdate, '/', '-'), 'yyyy-MM') AS visitmonth,
              visitcount
          FROM
              test1) t1
     GROUP BY
         userid,
         visitmonth) t2
ORDER BY
    t2.userid,
    t2.visitmonth;
/*
u01	2017-01	11	11
u01	2017-02	12	23
u02	2017-01	12	12
u03	2017-01	8	8
u04	2017-01	3	3
*/

