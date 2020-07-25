-- 练习 range 和 row 的差异
-- MySQL 8.0 关于这一块的 wiki: https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
-- #### 测试 Hive 窗口函数和 MySQL 窗口函数的区别

-- 1. 区分 range 和 rows 条件相同
select
    create_date,
    amount,
    sum(amount) over (order by create_date range between unbounded preceding and current row) as range_sum,
    sum(amount) over (order by create_date rows between unbounded preceding and current row)  as rows_sum
from
    order_tab;
/*
2018-01-01 100	100	100
2018-01-02 300	900	400
2018-01-02 500	900	900
2018-01-03 800	2200	1700
2018-01-03 500	2200	2200
2018-01-04 900	3700	3100
2018-01-04 600	3700	3700
2018-01-10 300	4000	4000
2018-01-16 800	4800	4800
2018-01-22 800	5600	5600
*/

/*
可以观察到使用 range 时为范围比较，即如果 order by 字段相等时呈现出来的 sum 值亦相等
而 rows 为条目比较，即使 order by 字段相等，呈现出来的 sum 也不相等
*/

-- 2. range 使用 INTERVAL 语法
-- #### Hive 不支持 range 中的 interval 语法 ####
