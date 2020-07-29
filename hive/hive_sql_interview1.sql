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

/*
第四题
需求
有一个5000万的用户文件(user_id，name，age)，
一个2亿记录的用户看电影的记录文件(user_id，url)，
根据年龄段观看电影的次数进行排序？
*/

CREATE TABLE test4user
(
    user_id string,
    name    string,
    age     int
);

CREATE TABLE test4log
(
    user_id string,
    url     string
);

INSERT INTO TABLE
    test4user
VALUES
    ('001', 'u1', 10),
    ('002', 'u2', 15),
    ('003', 'u3', 15),
    ('004', 'u4', 20),
    ('005', 'u5', 25),
    ('006', 'u6', 35),
    ('007', 'u7', 40),
    ('008', 'u8', 45),
    ('009', 'u9', 50),
    ('0010', 'u10', 65);

INSERT INTO TABLE
    test4log
VALUES
    ('001', 'url1'),
    ('002', 'url1'),
    ('003', 'url2'),
    ('004', 'url3'),
    ('005', 'url3'),
    ('006', 'url1'),
    ('007', 'url5'),
    ('008', 'url7'),
    ('009', 'url5'),
    ('0010', 'url1');


SELECT
    t2.age_phase,
    sum(t1.cnt) as view_cnt
FROM

    (SELECT
         user_id,
         count(*) cnt
     FROM
         test4log
     GROUP BY
         user_id) t1
        JOIN
        (SELECT
             user_id,
             CASE
                 WHEN age <= 10 AND age > 0 THEN '0-10'
                 WHEN age <= 20 AND age > 10 THEN '10-20'
                 WHEN age > 20 AND age <= 30 THEN '20-30'
                 WHEN age > 30 AND age <= 40 THEN '30-40'
                 WHEN age > 40 AND age <= 50 THEN '40-50'
                 WHEN age > 50 AND age <= 60 THEN '50-60'
                 WHEN age > 60 AND age <= 70 THEN '60-70'
                 ELSE '70以上' END as age_phase
         FROM
             test4user) t2 ON t1.user_id = t2.user_id
GROUP BY
    t2.age_phase;
/*
0-10	1
10-20	3
20-30	1
30-40	2
40-50	2
60-70	1
*/

/*
第五题
需求
有日志如下，请写出代码求得所有用户和活跃用户的总数及平均年龄。（活跃用户指连续两天都有访问记录的用户）
日期       用户     年龄
2019-02-11,test_1,23
2019-02-11,test_2,19
2019-02-11,test_3,39
2019-02-11,test_1,23
2019-02-11,test_3,39
2019-02-11,test_1,23
2019-02-12,test_2,19
2019-02-13,test_1,23
2019-02-15,test_2,19
2019-02-16,test_2,19
*/

CREATE TABLE test5
(
    dt      string,
    user_id string,
    age     int
);

INSERT INTO TABLE
    test5
VALUES
    ('2019-02-11', 'test_1', 23),
    ('2019-02-11', 'test_2', 19),
    ('2019-02-11', 'test_3', 39),
    ('2019-02-11', 'test_1', 23),
    ('2019-02-11', 'test_3', 39),
    ('2019-02-11', 'test_1', 23),
    ('2019-02-12', 'test_2', 19),
    ('2019-02-13', 'test_1', 23),
    ('2019-02-15', 'test_2', 19),
    ('2019-02-16', 'test_2', 19);

SELECT
    sum(total_user_cnt)     total_user_cnt,
    sum(total_user_avg_age) total_user_avg_age,
    sum(two_days_cnt)       two_days_cnt,
    sum(avg_age)            avg_age
FROM
    (SELECT
         0                                             total_user_cnt,
         0                                             total_user_avg_age,
         count(*)                                   AS two_days_cnt,
         cast(sum(age) / count(*) AS decimal(5, 2)) AS avg_age
     FROM
         (SELECT
              user_id,
              max(age) age
          FROM
              (SELECT
                   user_id,
                   max(age) age
               FROM
                   (SELECT
                        user_id,
                        age,
                        date_sub(dt, rank) flag
                    FROM
                        (SELECT
                             dt,
                             user_id,
                             max(age)         age,
                             row_number() over (PARTITION BY user_id
                                 ORDER BY dt) rank
                         FROM
                             test5
                         GROUP BY
                             dt,
                             user_id) t1) t2
               GROUP BY
                   user_id,
                   flag
               HAVING
                   count(*) >= 2) t3
          GROUP BY
              user_id) t4
     UNION ALL
     SELECT
         count(*)                                   total_user_cnt,
         cast(sum(age) / count(*) AS decimal(5, 2)) total_user_avg_age,
         0                                          two_days_cnt,
         0                                          avg_age
     FROM
         (SELECT
              user_id,
              max(age) age
          FROM
              test5
          GROUP BY
              user_id) t5) t6;
-- 3	27.00	1	19.00


/*
第六题
需求
请用sql写出所有用户中在今年10月份第一次购买商品的金额，
表ordertable字段:
(购买用户：userid，金额：money，购买时间：paymenttime(格式：2017-10-01)，订单id：orderid
*/

CREATE TABLE test6
(
    userid      string,
    money       decimal(10, 2),
    paymenttime string,
    orderid     string
);

INSERT INTO TABLE
    test6
VALUES
    ('001', 100, '2017-10-01', '123'),
    ('001', 200, '2017-10-02', '124'),
    ('002', 500, '2017-10-01', '125'),
    ('001', 100, '2017-11-01', '126');

SELECT
    userid,
    paymenttime,
    money,
    orderid
from
    (SELECT
         userid,
         money,
         paymenttime,
         orderid,
         row_number() over (PARTITION BY userid ORDER BY paymenttime) rank
     FROM
         test6
     WHERE
         date_format(paymenttime, 'yyyy-MM') = '2017-10'
    ) t
WHERE
    rank = 1;
/*
001	2017-10-01	100.00	123
002	2017-10-01	500.00	125
*/

/*
第七题
需求
现有图书管理数据库的三个数据模型如下：
图书（数据表名：BOOK）
	序号  	字段名称    字段描述    字段类型
	1   	BOOK_ID 	总编号 		文本
	2   	SORT    	分类号 		文本
	3  	 	BOOK_NAME   书名  		文本
	4   	WRITER  	作者  		文本
	5   	OUTPUT  	出版单位    文本
	6   	PRICE   	单价  		数值（保留小数点后2位）
读者（数据表名：READER）
	序号  	字段名称    字段描述    字段类型
	1   	READER_ID   借书证号    文本
	2   	COMPANY 	单位  		文本
	3   	NAME    	姓名  		文本
	4   	SEX 		性别  		文本
	5   	GRADE   	职称  		文本
	6   	ADDR    	地址  		文本
借阅记录（数据表名：BORROW LOG）
	序号  	字段名称    	字段描述    字段类型
	1   	READER_ID   	借书证号    文本
	2   	BOOK_ID  		总编号 		文本
	3   	BORROW_DATE  	借书日期    日期
（1）创建图书管理库的图书、读者和借阅三个基本表的表结构。请写出建表语句。
（2）找出姓李的读者姓名（NAME）和所在单位（COMPANY）。
（3）查找“高等教育出版社”的所有图书名称（BOOK_NAME）及单价（PRICE），结果按单价降序排序。
（4）查找价格介于10元和20元之间的图书种类(SORT）出版单位（OUTPUT）和单价（PRICE），结果按出版单位（OUTPUT）和单价（PRICE）升序排序。
（5）查找所有借了书的读者的姓名（NAME）及所在单位（COMPANY）。
（6）求”科学出版社”图书的最高单价、最低单价、平均单价。
（7）找出当前至少借阅了2本图书（大于等于2本）的读者姓名及其所在单位。
（8）考虑到数据安全的需要，需定时将“借阅记录”中数据进行备份，请使用一条SQL语句，
    在备份用户bak下创建与“借阅记录”表结构完全一致的数据表BORROW_LOG_BAK.井且将“借阅记录”中现有数据全部复制到BORROW_L0G_ BAK中。
（9）现在需要将原Oracle数据库中数据迁移至Hive仓库，请写出“图书”在Hive中的建表语句（Hive实现，提示：列分隔符|；
    数据表数据需要外部导入：分区分别以month＿part、day＿part 命名）
（10）Hive中有表A，现在需要将表A的月分区　201505　中　user＿id为20000的user＿dinner字段更新为bonc8920，
    其他用户user＿dinner字段数据不变，请列出更新的方法步骤。（Hive实现，提示：Hlive中无update语法，请通过其他办法进行数据更新）
*/

-- (1)
-- 创建图书表book

CREATE TABLE test7_book
(
    book_id   string,
    `SORT`    string,
    book_name string,
    writer    string,
    OUTPUT    string,
    price     decimal(10, 2)
);
INSERT INTO TABLE
    test7_book
VALUES
    ('001', 'TP391', '信息处理', 'author1', '机械工业出版社', '20'),
    ('002', 'TP392', '数据库', 'author12', '科学出版社', '15'),
    ('003', 'TP393', '计算机网络', 'author3', '机械工业出版社', '29'),
    ('004', 'TP399', '微机原理', 'author4', '科学出版社', '39'),
    ('005', 'C931', '管理信息系统', 'author5', '机械工业出版社', '40'),
    ('006', 'C932', '运筹学', 'author6', '科学出版社', '55');


-- 创建读者表reader

CREATE TABLE test7_reader
(
    reader_id string,
    company   string,
    name      string,
    sex       string,
    grade     string,
    addr      string
);
INSERT INTO TABLE
    test7_reader
VALUES
    ('0001', '阿里巴巴', 'jack', '男', 'vp', 'addr1'),
    ('0002', '百度', 'robin', '男', 'vp', 'addr2'),
    ('0003', '腾讯', 'tony', '男', 'vp', 'addr3'),
    ('0004', '京东', 'jasper', '男', 'cfo', 'addr4'),
    ('0005', '网易', 'zhangsan', '女', 'ceo', 'addr5'),
    ('0006', '搜狐', 'lisi', '女', 'ceo', 'addr6');

-- 创建借阅记录表borrow_log

CREATE TABLE test7_borrow_log
(
    reader_id   string,
    book_id     string,
    borrow_date string
);

INSERT INTO TABLE
    test7_borrow_log
VALUES
    ('0001', '002', '2019-10-14'),
    ('0002', '001', '2019-10-13'),
    ('0003', '005', '2019-09-14'),
    ('0004', '006', '2019-08-15'),
    ('0005', '003', '2019-10-10'),
    ('0006', '004', '2019-17-13');

-- (2)
SELECT
    name,
    company
FROM
    test7_reader
WHERE
    name LIKE '李%';

-- (3)
SELECT
    book_name,
    price
FROM
    test7_book
WHERE
    OUTPUT = "高等教育出版社"
ORDER BY
    price DESC;

-- (4)
SELECT
    sort,
    output,
    price
FROM
    test7_book
WHERE
      price >= 10
  and price <= 20
ORDER BY
    output,
    price;

-- (5)
SELECT
    b.name,
    b.company
FROM
    test7_borrow_log a
        JOIN test7_reader b ON a.reader_id = b.reader_id;

-- (6)
SELECT
    max(price),
    min(price),
    avg(price)
FROM
    test7_book
WHERE
    OUTPUT = '科学出版社';

-- (7)
SELECT
    b.name,
    b.company
FROM
    (SELECT
         reader_id
     FROM
         test7_borrow_log
     GROUP BY
         reader_id
     HAVING
         count(*) >= 2) a
        JOIN test7_reader b ON a.reader_id = b.reader_id;

-- (8)
CREATE TABLE test7_borrow_log_bak AS
SELECT *
FROM
    test7_borrow_log;

-- (9)
CREATE TABLE book_hive
(
    book_id   string,
    SORT      string,
    book_name string,
    writer    string,
    OUTPUT    string,
    price     DECIMAL(10, 2)
)
    partitioned BY ( month_part string, day_part string )
    ROW format delimited FIELDS TERMINATED BY '\\|' stored AS textfile;

/*
(10)
方式1：配置hive支持事务操作，分桶表，orc存储格式
方式2：第一步找到要更新的数据，将要更改的字段替换为新的值，第二步找到不需要更新的数据，第三步将上两步的数据插入一张新表中。
*/


/*
第八题
需求
有一个线上服务器访问日志格式如下（用sql答题）
时间                    接口                         ip地址
2016-11-09 14:22:05		/api/user/login				110.23.5.33
2016-11-09 14:23:10		/api/user/detail			57.3.2.16
2016-11-09 15:59:40		/api/user/login				200.6.5.166
… …
求11月9号下午14点（14-15点），访问/api/user/login接口的top10的ip地址
*/

CREATE TABLE test8
(
    `date`    string,
    interface string,
    ip        string
);

INSERT INTO TABLE
    test8
VALUES
    ('2016-11-09 11:22:05', '/api/user/login', '110.23.5.23'),
    ('2016-11-09 11:23:10', '/api/user/detail', '57.3.2.16'),
    ('2016-11-09 23:59:40', '/api/user/login', '200.6.5.166'),
    ('2016-11-09 11:14:23', '/api/user/login', '136.79.47.70'),
    ('2016-11-09 11:15:23', '/api/user/detail', '94.144.143.141'),
    ('2016-11-09 11:16:23', '/api/user/login', '197.161.8.206'),
    ('2016-11-09 12:14:23', '/api/user/detail', '240.227.107.145'),
    ('2016-11-09 13:14:23', '/api/user/login', '79.130.122.205'),
    ('2016-11-09 14:14:23', '/api/user/detail', '65.228.251.189'),
    ('2016-11-09 14:15:23', '/api/user/detail', '245.23.122.44'),
    ('2016-11-09 14:17:23', '/api/user/detail', '22.74.142.137'),
    ('2016-11-09 14:19:23', '/api/user/detail', '54.93.212.87'),
    ('2016-11-09 14:20:23', '/api/user/detail', '218.15.167.248'),
    ('2016-11-09 14:24:23', '/api/user/detail', '20.117.19.75'),
    ('2016-11-09 15:14:23', '/api/user/login', '183.162.66.97'),
    ('2016-11-09 16:14:23', '/api/user/login', '108.181.245.147'),
    ('2016-11-09 14:17:23', '/api/user/login', '22.74.142.137'),
    ('2016-11-09 14:19:23', '/api/user/login', '22.74.142.137');

SELECT
    ip,
    count(*) AS cnt
FROM
    test8
WHERE
      date_format(`date`, 'yyyy-MM-dd HH') >= '2016-11-09 14'
  AND date_format(`date`, 'yyyy-MM-dd HH') < '2016-11-09 15'
  AND interface = '/api/user/login'
GROUP BY
    ip
ORDER BY
    cnt desc
LIMIT 10;
-- 22.74.142.137	2

/*
第九题
需求
有一个充值日志表credit_log，字段如下：

`dist_id` int  '区组id',
`account` string  '账号',
`money` int   '充值金额',
`create_time` string  '订单时间'

请写出SQL语句，查询充值日志表2019年01月02号每个区组下充值额最大的账号，要求结果：
区组id，账号，金额，充值时间
*/

CREATE TABLE test9
(
    dist_id     string COMMENT '区组id',
    account     string COMMENT '账号',
    `money`     decimal(10, 2) COMMENT '充值金额',
    create_time string COMMENT '订单时间'
);

INSERT INTO TABLE
    test9
VALUES
    ('1', '11', 100006, '2019-01-02 13:00:01'),
    ('1', '22', 110000, '2019-01-02 13:00:02'),
    ('1', '33', 102000, '2019-01-02 13:00:03'),
    ('1', '44', 100300, '2019-01-02 13:00:04'),
    ('1', '55', 100040, '2019-01-02 13:00:05'),
    ('1', '66', 100005, '2019-01-02 13:00:06'),
    ('1', '77', 180000, '2019-01-03 13:00:07'),
    ('1', '88', 106000, '2019-01-02 13:00:08'),
    ('1', '99', 100400, '2019-01-02 13:00:09'),
    ('1', '12', 100030, '2019-01-02 13:00:10'),
    ('1', '13', 100003, '2019-01-02 13:00:20'),
    ('1', '14', 100020, '2019-01-02 13:00:30'),
    ('1', '15', 100500, '2019-01-02 13:00:40'),
    ('1', '16', 106000, '2019-01-02 13:00:50'),
    ('1', '17', 100800, '2019-01-02 13:00:59'),
    ('2', '18', 100800, '2019-01-02 13:00:11'),
    ('2', '19', 100030, '2019-01-02 13:00:12'),
    ('2', '10', 100000, '2019-01-02 13:00:13'),
    ('2', '45', 100010, '2019-01-02 13:00:14'),
    ('2', '78', 100070, '2019-01-02 13:00:15');

WITH
    TEMP AS
        (SELECT
             dist_id,
             account,
             sum(`money`) sum_money
         FROM
             test9
         WHERE
             date_format(create_time, 'yyyy-MM-dd') = '2019-01-02'
         GROUP BY
             dist_id,
             account)
SELECT
    t1.dist_id,
    t1.account,
    t1.sum_money
FROM
    (SELECT
         temp.dist_id,
         temp.account,
         temp.sum_money,
         rank() over (partition BY temp.dist_id
             ORDER BY temp.sum_money DESC) ranks
     FROM
         TEMP) t1
WHERE
    ranks = 1;
/*
1	22	110000.00
2	18	100800.00
*/


/*
第十题
需求
有一个账号表如下，请写出SQL语句，查询各自区组的money排名前十的账号（分组取前10）
dist_id string  '区组id',
account string  '账号',
gold     int    '金币'
*/
CREATE TABLE test10
(
    `dist_id` string COMMENT '区组id',
    `account` string COMMENT '账号',
    `gold`    int COMMENT '金币'
);

INSERT INTO TABLE
    test10
VALUES
    ('1', '77', 18),
    ('1', '88', 106),
    ('1', '99', 10),
    ('1', '12', 13),
    ('1', '13', 14),
    ('1', '14', 25),
    ('1', '15', 36),
    ('1', '16', 12),
    ('1', '17', 158),
    ('2', '18', 12),
    ('2', '19', 44),
    ('2', '10', 66),
    ('2', '45', 80),
    ('2', '78', 98);

SELECT
    dist_id,
    account,
    gold
FROM
    (SELECT
         dist_id,
         account,
         gold,
         row_number() over (PARTITION BY dist_id ORDER BY gold DESC) rank
     FROM
         test10) t
WHERE
    rank <= 10;
/*
1	17	158
1	88	106
1	15	36
1	14	25
1	77	18
1	13	14
1	12	13
1	16	12
1	99	10
2	78	98
2	45	80
2	10	66
2	19	44
2	18	12
*/

