-- 对于 大数据分析师入门6-HIVE进阶 https://mp.weixin.qq.com/s/c2Jmmxi5q8Nogllk7zsBjA 的实践
-- hive 窗口函数官网地址：https://cwiki.apache.org/confluence/display/Hive/LanguageManual+WindowingAndAnalytics
drop table windows_product;

create table if not exists windows_product
(
    product_id   string,
    product_name string,
    product_type string,
    sale_price   int
);


INSERT INTO TABLE windows_product
select
    '0001', 'T恤衫', '衣服', 1000
UNION ALL
select
    '0002', '打孔器', '办公用品', 500
UNION ALL
select
    '0003', '运动T恤', '衣服', 4000
UNION ALL
select
    '0004', '菜刀', '厨房用品', 3000
UNION ALL
select
    '0005', '高压锅', '厨房用品', 6800
UNION ALL
select
    '0006', '叉子', '厨房用品', 500
UNION ALL
select
    '0007', '插菜板', '厨房用品', 880
UNION ALL
select
    '0008', '圆珠笔', '办公用品', 100;

select * from windows_product;

select *
from
    windows_product;

-- 用于辅助计算：主要的用法是在原有表的基础上，增加一列聚合后的值，辅以后续的计算
-- 统计出不同产品类型售价最高的产品
-- 第一步：开窗增加辅助列
select
    product_name,
    product_type,
    sale_price,
    -- 增加一列作为聚合后的最高售价
    max(sale_price) over (partition by product_type) as max_sale_price
from
    windows_product;
/*
圆珠笔	办公用品	100	500
打孔器	办公用品	500	500
插菜板	厨房用品	880	6800
叉子	厨房用品	500	6800
高压锅	厨房用品	6800	6800
菜刀	厨房用品	3000	6800
运动T恤	衣服	4000	4000
T恤衫	衣服	1000	4000
*/

-- 第二步：保留与最高售价相同的条目
select
    t.product_type,
    t.product_name
from
    (
        select
            product_name,
            product_type,
            sale_price,
            max(sale_price) over (partition by product_type) as max_sale_price
        from
            windows_product
    ) t
where
    t.sale_price = t.max_sale_price;
/*
办公用品	打孔器
厨房用品	高压锅
衣服	运动T恤
*/


-- 累积计算：标准聚合函数作为窗口函数配合order by使用，可以实现累积计算。
-- sum窗口函数配合order by，可以实现累积和。
select
    product_id,
    sale_price,
    sum(sale_price) over (order by product_id) as current_sum
from
    windows_product;
/*
0001	1000	1000
0002	500	1500
0003	4000	5500
0004	3000	8500
0005	6800	15300
0006	500	15800
0007	880	16680
0008	100	16780
*/

-- 相应的AVG窗口函数配合order by，可以实现累积平均，max可以实现累积最大值，min可以实现累积最小值，count则可以实现累积计数。
-- 注意，只有计算类的窗口函数可以实现累积计算。
-- 标准聚合函数作为窗口函数使用的时候，在指明order by的情况下，如果没有Window子句，则Window子句默认为：
-- RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW(上边界不限制，下边界到当前行)。

select
    product_id,
    sale_price,
    sum(sale_price) over (order by product_id) as current_sum,
    avg(sale_price) over (order by product_id) as current_avg,
    max(sale_price) over (order by product_id) as current_max,
    min(sale_price) over (order by product_id) as current_min,
    count(sale_price) over (order by product_id) as current_count
from
    windows_product;
/*
0001	1000	1000	1000	1000	1000	1
0002	500	1500	750	1000	500	2
0003	4000	5500	1833.3333333333333	4000	500	3
0004	3000	8500	2125	4000	500	4
0005	6800	15300	3060	6800	500	5
0006	500	15800	2633.3333333333335	6800	500	6
0007	880	16680	2382.8571428571427	6800	500	7
0008	100	16780	2097.5	6800	100	8
*/

-- 移动计算：是在分区和排序的基础上，对计算范围进一步做出限定。
-- 按照产品ID排序，将最近3条的销售价格进行汇总平均。
select
    product_id,
    sale_price,
    -- 向上取最近两行的值求平均价格（第一第二行取前值为0，平均数分母相应减一即可）
    avg(sale_price) over (order by product_id rows 2 preceding) as moving_upper_avg
from
    windows_product;
/*
0001	1000	1000                (1000/1)
0002	500	750                     ((1000 + 750)/2)
0003	4000	1833.3333333333333  ((1000 + 750 + 4000)/3)
0004	3000	2500                ((750 + 4000 + 2500)/3)
0005	6800	4600                ((4000 + 3000 + 6800)/3)
0006	500	3433.3333333333335      ((3000 + 6800 + 500)/3)
0007	880	2726.6666666666665      ((6800 + 500 + 880)/3)
0008	100	493.3333333333333       ((500 + 880 + 100)/3)
*/

-- 取一字段值
-- 取值的窗口函数有：first_value/last_value、lag/lead。
-- first_value(字段名)-取出分区中的第一条记录的任意一个字段的值，可以排序也可以不排序，此处也可以进一步指明Window子句。
-- lag(字段名,N,默认值)-取出当前行之上的第N条记录的任意一个字段的值，这里的N和默认值都是可选的，默认N为1，默认值为null。
-- 使用first_value取出每个分类下的最贵的产品，如下：
select
    -- 注意，这里的 distinct 是对所有数据去重，并非只对 product_type 去重，
    -- 可以参考这里：面试了几个程序员，发现他们对于 mysql 的 distinct 关键字都存在错误的理解 https://v2ex.com/t/691611
    distinct product_type,
    first_value(product_name) over (partition by product_type order by sale_price desc) as max_price_product
from
    windows_product;
/*
办公用品	打孔器
厨房用品	高压锅
衣服	运动T恤
*/

-- 测试去掉 distinct 的 SQL
select
    product_type,
    first_value(product_name) over (partition by product_type order by sale_price desc) as max_price_product
from
    windows_product;
/*
办公用品	打孔器
办公用品	打孔器
厨房用品	高压锅
厨房用品	高压锅
厨房用品	高压锅
厨房用品	高压锅
衣服	运动T恤
衣服	运动T恤
*/

-- 排序
-- 排序对应的四个窗口函数为：rank、dense_rank、row_number、ntile
-- rank：计算排序时，如果存在相同位次的记录，则会跳过之后的位次。
-- e.g. 有三条记录排在第1位时：1位、1位、1位、4位......
-- dense_rank：计算排序时，即使存在相同位次的记录，也不会跳过之后的位次。
-- e.g. 有三条记录排在第1位时：1位、1位、1位、2位......
-- row_number：赋予唯一的连续位次。
-- e.g. 有三条记录排在第1位时：1位、2位、3位、4位...
-- ntile：用于将分组数据按照顺序切分成n片，返回当前切片值
-- e.g. 对于一组数字（1，2，3，4，5，6），ntile(2)切片后为（1，1，1，2，2，2）
-- 1）统计所有产品的售价排名
-- 具体代码如下：
select
    product_name,
    product_type,
    sale_price,
    rank() over (order by sale_price) as ranking
from
    windows_product;
/*
圆珠笔	办公用品	100	1
叉子	厨房用品	500	2
打孔器	办公用品	500	2
插菜板	厨房用品	880	4
T恤衫	衣服	1000	5
菜刀	厨房用品	3000	6
运动T恤	衣服	4000	7
高压锅	厨房用品	6800	8
*/

-- 2）统计各产品类型下各产品的售价排名
-- 具体代码如下：
select
    product_name,
    product_type,
    sale_price,
    rank() over (partition by product_type order by sale_price) as ranking
from
    windows_product;
/*
圆珠笔	办公用品	100	1
打孔器	办公用品	500	2
叉子	厨房用品	500	1
插菜板	厨房用品	880	2
菜刀	厨房用品	3000	3
高压锅	厨房用品	6800	4
T恤衫	衣服	1000	1
运动T恤	衣服	4000	2
*/

-- 对比一下 rank、dense_rank、row_number、ntile
-- 具体代码如下：
select
    product_name,
    product_type,
    sale_price,
    rank() over (order by sale_price) as ranking,
    dense_rank() over (order by sale_price) as dense_ranking,
    row_number() over (order by sale_price) as row_number,
    -- 切片小于总记录数
    ntile(3) over (order by sale_price) as slices_low,
    -- 切片大于总记录数
    ntile(30) over (order by sale_price) as slices_high
from
    windows_product;
/*
圆珠笔	办公用品	100	1	1	1	1	1
叉子	厨房用品	500	2	2	2	1	2
打孔器	办公用品	500	2	2	3	1	3
插菜板	厨房用品	880	4	3	4	2	4
T恤衫	衣服	1000	5	4	5	2	5
菜刀	厨房用品	3000	6	5	6	2	6
运动T恤	衣服	4000	7	6	7	3	7
高压锅	厨房用品	6800	8	7	8	3	8
*/
-- 从结果可以发现，当ntile(30)中的切片大于了总记录数时，切片的值为记录的序号。

-- 序列
-- 序列中的两个窗口函数cume_dist和percent_rank，通过实例来看看它们是怎么使用的。
-- 1）统计小于等于当前售价的产品数，所占总产品数的比例
-- 具体代码如下：
select
    product_type,
    product_name,
    sale_price,
    cume_dist() over (order by sale_price) as rn1,
    cume_dist() over (partition by product_type order by sale_price) as rn2
from
    windows_product;
/*
办公用品	圆珠笔	100	0.125	0.5
办公用品	打孔器	500	0.375	1
厨房用品	叉子	500	0.375	0.25
厨房用品	插菜板	880	0.5	0.5
厨房用品	菜刀	3000	0.75	0.75
厨房用品	高压锅	6800	1	1
衣服	T恤衫	1000	0.625	0.5
衣服	运动T恤	4000	0.875	1
*/
-- rn1: 没有partition,所有数据均为1组，总行数为8，
--      第一行：小于等于100的行数为1，因此，1/8=0.125
--      第二行：小于等于500的行数为3，因此，3/8=0.375
-- rn2: 按照产品类型分组，product_type=厨房用品的行数为4,
--      第三行：小于等于500的行数为1，因此，1/4=0.25

-- 2）统计每个产品的百分比排序
-- 当前行的RANK值-1/分组内总行数-1
-- 具体代码如下：
select
    product_type,
    product_name,
    sale_price,
    percent_rank() over (order by sale_price) as rn1,
    percent_rank() over (partition by product_type order by sale_price) as rn2
from
    windows_product;
/*
办公用品	圆珠笔	100	0	0
办公用品	打孔器	500	0.14285714285714285	1
厨房用品	叉子	500	0.14285714285714285	0
厨房用品	插菜板	880	0.42857142857142855	0.3333333333333333
厨房用品	菜刀	3000	0.7142857142857143	0.6666666666666666
厨房用品	高压锅	6800	1	1
衣服	T恤衫	1000	0.5714285714285714	0
衣服	运动T恤	4000	0.8571428571428571	1
*/
-- rn1: 没有partition,所有数据均为1组，总行数为8，
-- 第一行：排序为1，因此，（1-1）/（8-1）= 0
-- 第二行：排序为2，因此，（2-1）/（8-1）= 0.14
-- rn2: 按照产品类型分组，product_type=厨房用品的行数为4,
-- 第三行：排序为1，因此，（1-1）/（4-1）= 0
-- 第四行：排序为1，因此，（2-1）/（4-1）= 0.33

-- 测试 Hive 开窗函数为 over 子句起别名
select
    product_type,
    product_name,
    sale_price,
    percent_rank() over w as rn1,
    percent_rank() over s as rn2
from
    windows_product
window
    w as (order by sale_price),
    s as (partition by product_type order by sale_price);
/*
办公用品	圆珠笔	100	0	0
办公用品	打孔器	500	0.14285714285714285	1
厨房用品	叉子	500	0.14285714285714285	0
厨房用品	插菜板	880	0.42857142857142855	0.3333333333333333
厨房用品	菜刀	3000	0.7142857142857143	0.6666666666666666
厨房用品	高压锅	6800	1	1
衣服	T恤衫	1000	0.5714285714285714	0
衣服	运动T恤	4000	0.8571428571428571	1
*/
