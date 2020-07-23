-- https://www.jianshu.com/p/3f3cf58472ca 练习
-- test_window1
-- 分析函数 over(partition by 列名 order by 列名 rows between 开始位置 and 结束位置)
/*
PRECEDING：往前
FOLLOWING：往后
CURRENT ROW：当前行
UNBOUNDED：起点（一般结合PRECEDING，FOLLOWING使用）
UNBOUNDED PRECEDING 表示该窗口最前面的行（起点）
UNBOUNDED FOLLOWING：表示该窗口最后面的行（终点）
比如说：
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW（表示从起点到当前行）
ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING（表示往前2行到往后1行）
ROWS BETWEEN 2 PRECEDING AND 1 CURRENT ROW（表示往前2行到当前行）
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING（表示当前行到终点）
*/

drop table test_window1;

create table test_window1
(
    log_day   string,
    user_name string,
    score     int
);

insert into table
    test_window1
values
    ('20191020', 'tom', 85),
    ('20191020', 'jack', 83),
    ('20191020', 'nancy', 86),
    ('20191021', 'tom', 87),
    ('20191021', 'jack', 65),
    ('20191021', 'nancy', 98),
    ('20191022', 'tom', 67),
    ('20191022', 'jack', 34),
    ('20191022', 'nancy', 88),
    ('20191023', 'tom', 99),
    ('20191023', 'jack', 33);

-- 1、使用 over() 函数进行数据统计, 统计每个用户及表中数据的总数
--  即为每一行后面新增一个总条数
select
    log_day,
    user_name,
    score,
    count() over () all_records_count
from
    test_window1;
/*
20191023	jack	33	11
20191023	tom	99	11
20191022	nancy	88	11
20191022	jack	34	11
20191022	tom	67	11
20191021	nancy	98	11
20191021	jack	65	11
20191021	tom	87	11
20191020	nancy	86	11
20191020	jack	83	11
20191020	tom	85	11
*/

-- 2、求用户明细并统计每天的用户总数
--  即首先按照日期开窗，再为后面新增此日期的总条数
select
    log_day,
    user_name,
    score,
    count() over (partition by log_day) day_records_count
from
    test_window1;
/*
20191020	nancy	86	3
20191020	jack	83	3
20191020	tom	85	3
20191021	nancy	98	3
20191021	jack	65	3
20191021	tom	87	3
20191022	nancy	88	3
20191022	jack	34	3
20191022	tom	67	3
20191023	jack	33	2
20191023	tom	99	2
*/

-- 3、计算从第一天到现在的所有 score 大于80分的用户总数
-- 4、计算每个用户到当前日期分数大于80的天数