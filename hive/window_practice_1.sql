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

insert into table test_window1
select
    '20191020', 'tom', 85
UNION ALL
select
    '20191020', 'jack', 83
UNION ALL
select
    '20191020', 'nancy', 86
UNION ALL
select
    '20191021', 'tom', 87
UNION ALL
select
    '20191021', 'jack', 65
UNION ALL
select
    '20191021', 'nancy', 98
UNION ALL
select
    '20191022', 'tom', 67
UNION ALL
select
    '20191022', 'jack', 34
UNION ALL
select
    '20191022', 'nancy', 88
UNION ALL
select
    '20191023', 'tom', 99
UNION ALL
select
    '20191023', 'jack', 33;

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
--  over 函数中不包括谓词逻辑，仍然需要 where 进行过滤。
select
    log_day,
    user_name,
    score,
    count() over (order by log_day rows between unbounded preceding and current row) as total
from
    test_window1
where
    score > 80;


-- 4、计算每个用户到当前日期分数大于80的天数
--  根据题目 3 的要求并对每个用户开窗，加上全局 order by 使得结果更有序。
select
    log_day,
    user_name,
    score,
    count() over (partition by user_name order by log_day rows between unbounded preceding and current row) as total
from
    test_window1
where
    score > 80
order by
    log_day,
    user_name;
/*
20191020	jack	83	1
20191020	nancy	86	1
20191020	tom	85	1
20191021	nancy	98	2
20191021	tom	87	2
20191022	nancy	88	3
20191023	tom	99	3
*/

drop table business_window;

create table business_window
(
    name       string,
    order_date string,
    cost       int
);

insert into table business_window
select
    'jack', '2017-01-01', 10
UNION ALL
select
    'tony', '2017-01-02', 15
UNION ALL
select
    'jack', '2017-02-03', 23
UNION ALL
select
    'tony', '2017-01-04', 29
UNION ALL
select
    'jack', '2017-01-05', 46
UNION ALL
select
    'jack', '2017-04-06', 42
UNION ALL
select
    'tony', '2017-01-07', 50
UNION ALL
select
    'jack', '2017-01-08', 55
UNION ALL
select
    'mart', '2017-04-08', 62
UNION ALL
select
    'mart', '2017-04-09', 68
UNION ALL
select
    'neil', '2017-05-10', 12
UNION ALL
select
    'mart', '2017-04-11', 75
UNION ALL
select
    'neil', '2017-06-12', 80
UNION ALL
select
    'mart', '2017-04-13', 94;

-- 1、查询在2017年4月份购买过的顾客及总人数
select
    name,
    order_date,
    cost,
    count() over () all_nums
from
    business_window
where
    substr(order_date, 1, 7) = '2017-04';
/*
mart	2017-04-13	94	5
mart	2017-04-11	75	5
mart	2017-04-09	68	5
mart	2017-04-08	62	5
jack	2017-04-06	42	5
*/

-- 2、查询顾客的购买明细及月购买总额
--  计算每月每个用户的消费金额
select
    name,
    order_date,
    cost,
    sum(cost) over (partition by name, substr(order_date, 1, 7)) total_cost
from
    business_window;
/*
jack	2017-01-05	46	111
jack	2017-01-08	55	111
jack	2017-01-01	10	111
jack	2017-02-03	23	23
jack	2017-04-06	42	42
mart	2017-04-13	94	299
mart	2017-04-11	75	299
mart	2017-04-09	68	299
mart	2017-04-08	62	299
neil	2017-05-10	12	12
neil	2017-06-12	80	80
tony	2017-01-04	29	94
tony	2017-01-02	15	94
tony	2017-01-07	50	94
*/

-- 3、查询顾客的购买明细及到目前为止每个顾客购买总金额
--  按照顾客分组、日期升序排序、组内每条数据将之前的金额累加
select
    name,
    order_date,
    cost,
    sum(cost) over (partition by name order by order_date rows between unbounded preceding and current row) now_acc_cost
from
    business_window;
/*
jack	2017-01-01	10	10
jack	2017-01-05	46	56
jack	2017-01-08	55	111
jack	2017-02-03	23	134
jack	2017-04-06	42	176
mart	2017-04-08	62	62
mart	2017-04-09	68	130
mart	2017-04-11	75	205
mart	2017-04-13	94	299
neil	2017-05-10	12	12
neil	2017-06-12	80	92
tony	2017-01-02	15	15
tony	2017-01-04	29	44
tony	2017-01-07	50	94
*/

-- 4、查询顾客上次的购买时间----lag()over()偏移量分析函数的运用
select
    name,
    order_date,
    cost,
    lag(order_date, 1) over (partition by name order by order_date) last_order_date
from
    business_window;
/*
jack	2017-01-01	10
jack	2017-01-05	46	2017-01-01
jack	2017-01-08	55	2017-01-05
jack	2017-02-03	23	2017-01-08
jack	2017-04-06	42	2017-02-03
mart	2017-04-08	62
mart	2017-04-09	68	2017-04-08
mart	2017-04-11	75	2017-04-09
mart	2017-04-13	94	2017-04-11
neil	2017-05-10	12
neil	2017-06-12	80	2017-05-10
tony	2017-01-02	15
tony	2017-01-04	29	2017-01-02
tony	2017-01-07	50	2017-01-04
*/

-- 5、查询前20%金额的订单信息
--  ntile 切片函数切割前五分之一
select
    name,
    order_date,
    cost
from
    (
        select
            name,
            order_date,
            cost,
            ntile(5) over (order by cost desc) sort_group_num
        from
            business_window
    ) t
where
    t.sort_group_num = 1;
/*
mart	2017-04-13	94
neil	2017-06-12	80
mart	2017-04-11	75
*/


create table score_window
(
    name    string,
    subject string,
    score   int
);

insert into table score_window
select
    '孙悟空', '语文', 87
UNION ALL
select
    '孙悟空', '数学', 95
UNION ALL
select
    '孙悟空', '英语', 68
UNION ALL
select
    '大海', '语文', 94
UNION ALL
select
    '大海', '数学', 56
UNION ALL
select
    '大海', '英语', 84
UNION ALL
select
    '宋宋', '语文', 64
UNION ALL
select
    '宋宋', '数学', 86
UNION ALL
select
    '宋宋', '英语', 84
UNION ALL
select
    '婷婷', '语文', 65
UNION ALL
select
    '婷婷', '数学', 85
UNION ALL
select
    '婷婷', '英语', 78;

-- 1、每门学科学生成绩排名(是否并列排名、空位排名三种实现)
--  比较 rank、dense_rank、row_number
select
    name,
    subject,
    score,
    row_number() over (partition by subject order by score desc) rn,
    rank() over (partition by subject order by score desc)       r,
    dense_rank() over (partition by subject order by score desc) dr
from
    score_window;
/*
孙悟空	数学	95	1	1	1
宋宋	数学	86	2	2	2
婷婷	数学	85	3	3	3
大海	数学	56	4	4	4
宋宋	英语	84	1	1	1
大海	英语	84	2	1	1
婷婷	英语	78	3	3	2
孙悟空	英语	68	4	4	3
大海	语文	94	1	1	1
孙悟空	语文	87	2	2	2
婷婷	语文	65	3	3	3
宋宋	语文	64	4	4	4
*/

-- 2、每门学科成绩排名top n的学生
--  取 top 3 吧
select
    name,
    subject,
    score,
    top_n
from
    (
        select
            name,
            subject,
            score,
            row_number() over (partition by subject order by score desc) top_n
        from
            score_window
    ) t
where
    top_n <= 3;
/*
孙悟空	数学	95	1
宋宋	数学	86	2
婷婷	数学	85	3
宋宋	英语	84	1
大海	英语	84	2
婷婷	英语	78	3
大海	语文	94	1
孙悟空	语文	87	2
婷婷	语文	65	3
*/
