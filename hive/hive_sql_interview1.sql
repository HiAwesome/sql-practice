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


/*
第二题
需求
有50W个京东店铺，每个顾客访客访问任何一个店铺的任何一个商品时都会产生一条访问日志，
访问日志存储的表名为Visit，访客的用户id为user_id，被访问的店铺名称为shop，数据如下：

				u1	a
				u2	b
				u1	b
				u1	a
				u3	c
				u4	b
				u1	a
				u2	c
				u5	b
				u4	b
				u6	c
				u2	c
				u1	b
				u2	a
				u2	a
				u3	a
				u5	a
				u5	a
				u5	a
请统计：
(1)每个店铺的UV（访客数）
(2)每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
*/

CREATE TABLE test2
(
    user_id string,
    shop    string
);

INSERT INTO TABLE
    test2
VALUES
    ('u1', 'a'),
    ('u2', 'b'),
    ('u1', 'b'),
    ('u1', 'a'),
    ('u3', 'c'),
    ('u4', 'b'),
    ('u1', 'a'),
    ('u2', 'c'),
    ('u5', 'b'),
    ('u4', 'b'),
    ('u6', 'c'),
    ('u2', 'c'),
    ('u1', 'b'),
    ('u2', 'a'),
    ('u2', 'a'),
    ('u3', 'a'),
    ('u5', 'a'),
    ('u5', 'a'),
    ('u5', 'a');

SELECT
    shop,
    count(DISTINCT user_id) uv
FROM
    test2
GROUP BY
    shop;
/*
a	4
b	4
c	3
*/

SELECT
    t2.shop,
    t2.user_id,
    t2.cnt
FROM
    (SELECT
         t1.*,
         row_number() over (partition BY t1.shop ORDER BY t1.cnt DESC) rank
     FROM
         (SELECT
              user_id,
              shop,
              count(*) AS cnt
          FROM
              test2
          GROUP BY
              user_id,
              shop
         ) t1
    ) t2
WHERE
    rank <= 3;
/*
a	u5	3
a	u1	3
a	u2	2
b	u4	2
b	u1	2
b	u5	1
c	u2	2
c	u6	1
c	u3	1
*/


/*
第三题
需求
已知一个表STG.ORDER，有如下字段:Date，Order_id，User_id，amount。
数据样例:2017-01-01,10029028,1000003251,33.57。
请给出sql进行统计:
(1)给出 2017年每个月的订单数、用户数、总成交金额。
(2)给出2017年11月的新客数(指在11月才有第一笔订单)
*/

CREATE TABLE test3
(
    dt       string,
    order_id string,
    user_id  string,
    amount   DECIMAL(10, 2)
);

INSERT INTO TABLE
    test3
VALUES
    ('2017-01-01', '10029028', '1000003251', 33.57),
    ('2017-01-01', '10029029', '1000003251', 33.57),
    ('2017-01-01', '100290288', '1000003252', 33.57),
    ('2017-02-02', '10029088', '1000003251', 33.57),
    ('2017-02-02', '100290281', '1000003251', 33.57),
    ('2017-02-02', '100290282', '1000003253', 33.57),
    ('2017-11-02', '10290282', '100003253', 234),
    ('2018-11-02', '10290284', '100003243', 234);

SELECT
    t1.mon,
    count(t1.order_id)         AS order_cnt,
    count(DISTINCT t1.user_id) AS user_cnt,
    sum(amount)                AS total_amount
FROM
    (SELECT
         order_id,
         user_id,
         amount,
         date_format(dt, 'yyyy-MM') mon
     FROM
         test3
     WHERE
         date_format(dt, 'yyyy') = '2017') t1
GROUP BY
    t1.mon;
/*
2017-01	3	2	100.71
2017-02	3	2	100.71
2017-11	1	1	234.00
*/

SELECT
    count(user_id)
FROM
    test3
GROUP BY
    user_id
HAVING
    date_format(min(dt), 'yyyy-MM') = '2017-11';
-- 1

