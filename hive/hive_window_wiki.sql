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

-- 来源： https://blog.csdn.net/scgaliguodong123_/article/details/60135385
-- 补充说明：
/*
窗口函数与分析函数
应用场景：
（1）用于分区排序
（2）动态Group By
（3）Top N
（4）累计计算
（5）层次查询

窗口函数
FIRST_VALUE：取分组内排序后，截止到当前行，第一个值
LAST_VALUE： 取分组内排序后，截止到当前行，最后一个值
LEAD(col,n,DEFAULT) ：用于统计窗口内往下第n行值。第一个参数为列名，第二个参数为往下第n行（可选，默认为1），第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL）
LAG(col,n,DEFAULT) ：与lead相反，用于统计窗口内往上第n行值。第一个参数为列名，第二个参数为往上第n行（可选，默认为1），第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）

OVER从句
1、使用标准的聚合函数COUNT、SUM、MIN、MAX、AVG
2、使用PARTITION BY语句，使用一个或者多个原始数据类型的列
3、使用PARTITION BY与ORDER BY语句，使用一个或者多个数据类型的分区或者排序列
4、使用窗口规范，窗口规范支持以下格式：

(ROWS | RANGE) BETWEEN (UNBOUNDED | [num]) PRECEDING AND ([num] PRECEDING | CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROWS | RANGE) BETWEEN CURRENT ROW AND (CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROWS | RANGE) BETWEEN [num] FOLLOWING AND (UNBOUNDED | [num]) FOLLOWING

当ORDER BY后面缺少窗口从句条件，窗口规范默认是 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.

当ORDER BY和窗口从句都缺失, 窗口规范默认是 ROW BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING.

OVER从句支持以下函数， 但是并不支持和窗口一起使用它们。
Ranking函数: Rank, NTile, DenseRank, CumeDist, PercentRank.
Lead 和 Lag 函数.

分析函数
ROW_NUMBER() 从1开始，按照顺序，生成分组内记录的序列,比如，按照pv降序排列，生成分组内每天的pv名次,ROW_NUMBER()的应用场景非常多，再比如，获取分组内排序第一的记录;获取一个session中的第一条refer等。
RANK() 生成数据项在分组中的排名，排名相等会在名次中留下空位
DENSE_RANK() 生成数据项在分组中的排名，排名相等会在名次中不会留下空位
CUME_DIST 小于等于当前值的行数/分组内总行数。比如，统计小于等于当前薪水的人数，所占总人数的比例
PERCENT_RANK 分组内当前行的RANK值-1/分组内总行数-1
NTILE(n) 用于将分组数据按照顺序切分成n片，返回当前切片值，如果切片不均匀，默认增加第一个切片的分布。NTILE不支持ROWS BETWEEN，比如 NTILE(2) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)。

Hive2.1.0及以后支持Distinct
在聚合函数（SUM, COUNT and AVG）中，支持distinct，但是在ORDER BY 或者 窗口限制不支持。

COUNT(DISTINCT a) OVER (PARTITION BY c)
1
Hive 2.2.0中在使用ORDER BY和窗口限制时支持distinct

COUNT(DISTINCT a) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
1
Hive2.1.0及以后支持在OVER从句中支持聚合函数
SELECT rank() OVER (ORDER BY sum(b))
FROM T
GROUP BY a;
*/
