# 练习 range 和 row 的差异
# MySQL 8.0 关于这一块的 wiki: https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html

# 1. 区分 range 和 rows 条件相同
select
    create_date,
    amount,
    sum(amount) over (order by create_date range between unbounded preceding and current row) as range_sum,
    sum(amount) over (order by create_date rows between unbounded preceding and current row)  as rows_sum
from
    order_tab;
/*
2018-01-01 00:00:00	100	100	100
2018-01-02 00:00:00	300	900	400
2018-01-02 00:00:00	500	900	900
2018-01-03 00:00:00	800	2200	1700
2018-01-03 00:00:00	500	2200	2200
2018-01-04 00:00:00	900	3700	3100
2018-01-04 00:00:00	600	3700	3700
2018-01-10 00:00:00	300	4000	4000
2018-01-16 00:00:00	800	4800	4800
2018-01-22 00:00:00	800	5600	5600
*/

/*
可以观察到使用 range 时为范围比较，即如果 order by 字段相等时呈现出来的 sum 值亦相等
而 rows 为条目比较，即使 order by 字段相等，呈现出来的 sum 也不相等
*/

# 2. range 使用 INTERVAL 语法
select
    create_date,
    amount,
    sum(amount)
        over (order by create_date range between interval 1 day preceding and current row)         as internal_1_day_sum,
    sum(amount) over (order by create_date range between interval 2 day preceding and current row) as internal_2_day_sum
from
    order_tab;
/*
2018-01-01 00:00:00	100	100	100
2018-01-02 00:00:00	300	900	900
2018-01-02 00:00:00	500	900	900
2018-01-03 00:00:00	800	2100	2200
2018-01-03 00:00:00	500	2100	2200
2018-01-04 00:00:00	900	2800	3600
2018-01-04 00:00:00	600	2800	3600
2018-01-10 00:00:00	300	300	300
2018-01-16 00:00:00	800	800	800
2018-01-22 00:00:00	800	800	800
*/

select
    create_date,
    amount,
    sum(amount)
        over (order by create_date range between interval 1 day preceding and interval 1 day following)    as inter_sum1,
    sum(amount)
        over (order by create_date range between interval 1 day preceding and unbounded following)         as inter_sum2,
    sum(amount)
        over (order by create_date range between unbounded preceding and interval 1 day following)         as inter_sum3,
    sum(amount)
        over (order by create_date range between interval 1 day preceding and current row)                 as inter_sum4,
    sum(amount) over (order by create_date range between current row and interval 1 day following)         as inter_sum5
from
    order_tab;
/*
2018-01-01 00:00:00	100	900	5600	900	100	900
2018-01-02 00:00:00	300	2200	5600	2200	900	2100
2018-01-02 00:00:00	500	2200	5600	2200	900	2100
2018-01-03 00:00:00	800	3600	5500	3700	2100	2800
2018-01-03 00:00:00	500	3600	5500	3700	2100	2800
2018-01-04 00:00:00	900	2800	4700	3700	2800	1500
2018-01-04 00:00:00	600	2800	4700	3700	2800	1500
2018-01-10 00:00:00	300	300	1900	4000	300	300
2018-01-16 00:00:00	800	800	1600	4800	800	800
2018-01-22 00:00:00	800	800	800	5600	800	800
*/

/*
首先 rows 无法使用 interval 语法。
其次在 range 中使用 interval 语法，需要 order by 字段含有时间属性。
在 range 中使用 interval 语法需要将 preceding 和 following 写完整，即使需要补充 current row 或者 unbounded preceding/following，否则语法报错
*/
