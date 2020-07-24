# 对 https://dbaplus.cn/news-11-2258-1.html 的练习
# MySQL 8.0窗口函数：用非常规思维简易实现SQL需求
/*
团队介绍
网易乐得DBA组，负责网易乐得电商、网易邮箱、网易技术部数据库日常运维，负责数据库私有云平台的开发和维护，负责数据库及数据库中间件Cetus的开发和测试等等。

一、窗口函数的使用场景
作为IT人士，日常工作中经常会遇到类似这样的需求：
医院看病，怎样知道上次就医距现在的时间？环比如何计算？怎么样得到各部门工资排名前N名员工列表?查找各部门每人工资占部门总工资的百分比？
对于这样的需求，使用传统的SQL实现起来比较困难。这类需求都有一个共同的特点，需要在单表中满足某些条件的记录集内部做一些函数操作，不是简单的表连接，也不是简单的聚合可以实现的，通常会让写SQL的同学焦头烂额、绞尽脑汁，费了大半天时间写出来一堆长长的晦涩难懂的自连接SQL，且性能低下，难以维护。
要解决此类问题，最方便的就是使用窗口函数。

二、MySQL窗口函数简介
MySQL从8.0开始支持窗口函数，这个功能在大多商业数据库和部分开源数据库中早已支持，有的也叫分析函数。
什么叫窗口?
窗口的概念非常重要，它可以理解为记录集合，窗口函数也就是在满足某种条件的记录集合上执行的特殊函数。对于每条记录都要在此窗口内执行函数，有的函数随着记录不同，窗口大小都是固定的，这种属于静态窗口；有的函数则相反，不同的记录对应着不同的窗口，这种动态变化的窗口叫滑动窗口。
窗口函数和普通聚合函数也很容易混淆，二者区别如下：
聚合函数是将多条记录聚合为一条；而窗口函数是每条记录都会执行，有几条记录执行完还是几条。
聚合函数也可以用于窗口函数中，这个后面会举例说明。
*/

drop table order_tab;

create table order_tab
(
    order_id  int      not null
        primary key,
    user_no   char(10) not null,
    amount   int not null,
    create_date datetime not null
);

insert into order_tab
values
    (1, '001', 100, '2018-01-01 00:00:00'),
    (2, '001', 300, '2018-01-02 00:00:00'),
    (3, '001', 500, '2018-01-02 00:00:00'),
    (4, '001', 800, '2018-01-03 00:00:00'),
    (5, '001', 900, '2018-01-04 00:00:00'),
    (6, '002', 500, '2018-01-03 00:00:00'),
    (7, '002', 600, '2018-01-04 00:00:00'),
    (8, '002', 300, '2018-01-10 00:00:00'),
    (9, '002', 800, '2018-01-16 00:00:00'),
    (10, '002', 800, '2018-01-22 00:00:00');

select * from order_tab;


# 1. 显示每个用户按照订单金额从大到小排序的序号
select
    row_number() over (partition by user_no order by amount desc) as row_num,
    order_id,
    user_no,
    amount,
    create_date
from
    order_tab;
/*
1	5	001	900	2018-01-04 00:00:00
2	4	001	800	2018-01-03 00:00:00
3	3	001	500	2018-01-02 00:00:00
4	2	001	300	2018-01-02 00:00:00
5	1	001	100	2018-01-01 00:00:00
1	9	002	800	2018-01-16 00:00:00
2	10	002	800	2018-01-22 00:00:00
3	7	002	600	2018-01-04 00:00:00
4	6	002	500	2018-01-03 00:00:00
5	8	002	300	2018-01-10 00:00:00
*/

/*
按照功能划分，可以把MySQL支持的窗口函数分为如下几类：

序号函数：row_number() / rank() / dense_rank()
分布函数：percent_rank() / cume_dist()
前后函数：lag() / lead()
头尾函数：first_val() / last_val()
其他函数：nth_value() / ntile()

窗口函数的基本用法如下：

函数名（[expr]） over子句

其中，over是关键字，用来指定函数执行的窗口范围，如果后面括号中什么都不写，则意味着窗口包含满足where条件的所有行，窗口函数基于所有行进行计算；
如果不为空，则支持以下四种语法来设置窗口：
window_name：给窗口指定一个别名，如果SQL中涉及的窗口较多，采用别名可以看起来更清晰易读。
上面例子中如果指定一个别名w，则改写如下：
*/

select
    row_number() over window_alias as row_num,
    order_id,
    user_no,
    amount,
    create_date
from
    order_tab
window
    window_alias as (partition by user_no order by amount desc);
/*
1	5	001	900	2018-01-04 00:00:00
2	4	001	800	2018-01-03 00:00:00
3	3	001	500	2018-01-02 00:00:00
4	2	001	300	2018-01-02 00:00:00
5	1	001	100	2018-01-01 00:00:00
1	9	002	800	2018-01-16 00:00:00
2	10	002	800	2018-01-22 00:00:00
3	7	002	600	2018-01-04 00:00:00
4	6	002	500	2018-01-03 00:00:00
5	8	002	300	2018-01-10 00:00:00
*/


/*
partition子句：窗口按照那些字段进行分组，窗口函数在不同的分组上分别执行。上面的例子就按照用户id进行了分组。
在每个用户id上，按照order by的顺序分别生成从1开始的顺序编号。

order by子句：按照哪些字段进行排序，窗口函数将按照排序后的记录顺序进行编号。
可以和partition子句配合使用，也可以单独使用。
上例中二者同时使用，如果没有partition子句，则会按照所有用户的订单金额排序来生成序号。

frame子句：frame是当前分区的一个子集，子句用来定义子集的规则，通常用来作为滑动窗口使用。
比如要根据每个订单动态计算包括本订单和按时间顺序前后两个订单的平均订单金额，则可以设置如下frame子句来创建滑动窗口：
*/

select
    order_id,
    user_no,
    amount,
    avg(amount) over window_alias as avg_num,
    create_date
from
    order_tab
window
    window_alias as (partition by user_no order by create_date desc rows between 1 preceding and 1 following);
/*
5	001	900	850.0000	2018-01-04 00:00:00
4	001	800	666.6667	2018-01-03 00:00:00
2	001	300	533.3333	2018-01-02 00:00:00
3	001	500	300.0000	2018-01-02 00:00:00
1	001	100	300.0000	2018-01-01 00:00:00
10	002	800	800.0000	2018-01-22 00:00:00
9	002	800	633.3333	2018-01-16 00:00:00
8	002	300	566.6667	2018-01-10 00:00:00
7	002	600	466.6667	2018-01-04 00:00:00
6	002	500	550.0000	2018-01-03 00:00:00
*/