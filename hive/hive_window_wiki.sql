-- 官方 Wiki: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics

/*
默认窗口规则：
当指定 ORDER BY 缺少 WINDOW 子句时，WINDOW 规范默认为 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
当缺少 ORDER BY 和 WINDOW 子句时，WINDOW 规范默认为 ROW BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.
*/

-- 1、 指定 partition by 和 order by
select
    name,
    order_date,
    cost,
    sum(cost) over (partition by name order by order_date) all_cost,
    max(cost) over (partition by name order by order_date) max_cost,
    min(cost) over (partition by name order by order_date) min_cost,
    -- 这里加上一个 round 去掉 avg 运算冗长的小数部分
    round(avg(cost) over (partition by name order by order_date), 2) avg_cost,
    count(cost) over (partition by name order by order_date) count_cost
from
    business_window;
/*
jack	2017-01-01	10	10	10	10	10	1
jack	2017-01-05	46	56	46	10	28	2
jack	2017-01-08	55	111	55	10	37	3
jack	2017-02-03	23	134	55	10	33.5	4
jack	2017-04-06	42	176	55	10	35.2	5
mart	2017-04-08	62	62	62	62	62	1
mart	2017-04-09	68	130	68	62	65	2
mart	2017-04-11	75	205	75	62	68.33	3
mart	2017-04-13	94	299	94	62	74.75	4
neil	2017-05-10	12	12	12	12	12	1
neil	2017-06-12	80	92	80	12	46	2
tony	2017-01-02	15	15	15	15	15	1
tony	2017-01-04	29	44	29	15	22	2
tony	2017-01-07	50	94	50	15	31.33	3
*/

-- 2、 仅指定 partition by
select
    name,
    order_date,
    cost,
    sum(cost) over (partition by name) all_cost,
    max(cost) over (partition by name) max_cost,
    min(cost) over (partition by name) min_cost,
    -- 这里加上一个 round 去掉 avg 运算冗长的小数部分
    round(avg(cost) over (partition by name), 2) avg_cost,
    count(cost) over (partition by name) count_cost
from
    business_window;
/*
jack	2017-01-05	46	176	55	10	35.2	5
jack	2017-01-08	55	176	55	10	35.2	5
jack	2017-01-01	10	176	55	10	35.2	5
jack	2017-04-06	42	176	55	10	35.2	5
jack	2017-02-03	23	176	55	10	35.2	5
mart	2017-04-13	94	299	94	62	74.75	4
mart	2017-04-11	75	299	94	62	74.75	4
mart	2017-04-09	68	299	94	62	74.75	4
mart	2017-04-08	62	299	94	62	74.75	4
neil	2017-05-10	12	92	80	12	46	2
neil	2017-06-12	80	92	80	12	46	2
tony	2017-01-04	29	94	50	15	31.33	3
tony	2017-01-02	15	94	50	15	31.33	3
tony	2017-01-07	50	94	50	15	31.33	3
*/

-- 2、 over 内部为空
select
    name,
    order_date,
    cost,
    sum(cost) over () all_cost,
    max(cost) over () max_cost,
    min(cost) over () min_cost,
    -- 这里加上一个 round 去掉 avg 运算冗长的小数部分
    round(avg(cost) over (), 2) avg_cost,
    count(cost) over () count_cost
from
    business_window;
/*
mart	2017-04-13	94	661	94	10	47.21	14
neil	2017-06-12	80	661	94	10	47.21	14
mart	2017-04-11	75	661	94	10	47.21	14
neil	2017-05-10	12	661	94	10	47.21	14
mart	2017-04-09	68	661	94	10	47.21	14
mart	2017-04-08	62	661	94	10	47.21	14
jack	2017-01-08	55	661	94	10	47.21	14
tony	2017-01-07	50	661	94	10	47.21	14
jack	2017-04-06	42	661	94	10	47.21	14
jack	2017-01-05	46	661	94	10	47.21	14
tony	2017-01-04	29	661	94	10	47.21	14
jack	2017-02-03	23	661	94	10	47.21	14
tony	2017-01-02	15	661	94	10	47.21	14
jack	2017-01-01	10	661	94	10	47.21	14
*/

/*
OVER 子句支持以下功能，但不支持带有它们的窗口（请参见 HIVE-4797 https://issues.apache.org/jira/browse/HIVE-4797）：
排名函数：Rank, NTile, DenseRank, CumeDist, PercentRank.
Lead and Lag functions.

CumeDist 和 PercentRank 的练习见 windows_hive_function.sql 最后
适合完成当前数据在总数据中或细分组数据中位置的查询。

row_number 经测试不支持带有窗口。

支持带窗口的函数仅有：
计算函数：sum、count、avg、max、min
取值函数的两端：first_value、last_value
*/

-- first_value and window
select
    name,
    cost,
    -- 向前取一位
    first_value(cost) over (rows between 1 preceding and current row) f_cost1,
    -- 取最开头
    first_value(cost) over (rows between unbounded preceding and current row) f_cost2,
    -- 加排序
    first_value(cost) over (order by cost rows between unbounded preceding and current row) f_cost3,
    -- 加分区和排序
    first_value(cost) over (partition by name order by cost rows between unbounded preceding and current row) f_cost4
from
    business_window;
/*
jack	10	15	94	10	10
jack	23	29	94	10	10
jack	42	50	94	10	10
jack	46	42	94	10	10
jack	55	62	94	10	10
mart	62	68	94	10	62
mart	68	12	94	10	62
mart	75	80	94	10	62
mart	94	94	94	10	62
neil	12	75	94	10	12
neil	80	94	94	10	12
tony	15	23	94	10	15
tony	29	46	94	10	15
tony	50	55	94	10	15
*/

-- last_value and window
select
    name,
    cost,
    -- 向后取一位
    last_value(cost) over (rows between current row and 1 following) l_cost1,
    -- 取最后头
    last_value(cost) over (rows between current row and unbounded following) l_cost2,
    -- 加排序
    last_value(cost) over (order by cost rows between current row and unbounded following) l_cost3,
    -- 加分区和排序
    last_value(cost) over (partition by name order by cost rows between current row and unbounded following) l_cost4
from
    business_window;
/*
jack	10	10	10	94	55
jack	23	15	10	94	55
jack	42	46	10	94	55
jack	46	29	10	94	55
jack	55	50	10	94	55
mart	62	55	10	94	94
mart	68	62	10	94	94
mart	75	12	10	94	94
mart	94	80	10	94	94
neil	12	68	10	94	80
neil	80	75	10	94	80
tony	15	10	10	94	50
tony	29	23	10	94	50
tony	50	42	10	94	50
*/
