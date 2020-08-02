-- 题目要求将测试数据转化为结果数据：即把停留时间重叠的用户和地点进行合并
/*
测试数据：
user_a    location_a    2018-01-01 08:00:00    60
user_a    location_a    2018-01-01 09:00:00    60
user_a    location_a    2018-01-01 11:00:00    60
user_a    location_a    2018-01-01 12:00:00    60
user_a    location_b    2018-01-01 10:00:00    60
user_a    location_c    2018-01-01 08:00:00    60
user_a    location_c    2018-01-01 09:00:00    60
user_a    location_c    2018-01-01 10:00:00    60
user_b    location_a    2018-01-01 15:00:00    60
user_b    location_a    2018-01-01 16:00:00    60
user_b    location_a    2018-01-01 18:00:00    60
结果数据：
user_a    location_a    2018-01-01 08:00:00    120
user_a    location_a    2018-01-01 11:00:00    120
user_a    location_b    2018-01-01 10:00:00    60
user_a    location_c    2018-01-01 08:00:00    180
user_b    location_a    2018-01-01 15:00:00    120
user_b    location_a    2018-01-01 18:00:00    60
*/

drop table user_location;

-- 建表
CREATE TABLE IF NOT EXISTS user_location
(
    user_id     char(10) not null,
    location_id char(20) not null,
    u_dt        datetime,
    duration    int
);

-- 插入数据
insert into
    user_location
select
    'user_a',
    'location_a',
    '2018-01-01 08:00:00',
    60
UNION ALL
select
    'user_a',
    'location_a',
    '2018-01-01 09:00:00',
    60
UNION ALL
select
    'user_a',
    'location_a',
    '2018-01-01 11:00:00',
    60
UNION ALL
select
    'user_a',
    'location_a',
    '2018-01-01 12:00:00',
    60
UNION ALL
select
    'user_a',
    'location_b',
    '2018-01-01 10:00:00',
    60
UNION ALL
select
    'user_a',
    'location_c',
    '2018-01-01 08:00:00',
    60
UNION ALL
select
    'user_a',
    'location_c',
    '2018-01-01 09:00:00',
    60
UNION ALL
select
    'user_a',
    'location_c',
    '2018-01-01 10:00:00',
    60
UNION ALL
select
    'user_b',
    'location_a',
    '2018-01-01 15:00:00',
    60
UNION ALL
select
    'user_b',
    'location_a',
    '2018-01-01 16:00:00',
    60
UNION ALL
select
    'user_b',
    'location_a',
    '2018-01-01 18:00:00',
    60;

select *
from
    user_location;

# 观察可知 gap 相同的数据可以合并
select
    user_id,
    location_id,
    u_dt,
    duration,
    first_value(u_dt) over w as                                                                                   fv,
    row_number() over w      as                                                                                   rn,
    TIMESTAMPDIFF(hour, (first_value(u_dt) over w), u_dt)                                                         dd,
    cast((row_number() over w) as signed) - cast(TIMESTAMPDIFF(hour, (first_value(u_dt) over w), u_dt) as signed) gap
from
    user_location
    window
        w as (partition by user_id, location_id order by u_dt);
/*
user_a	location_a	2018-01-01 08:00:00	60	2018-01-01 08:00:00	1	0	1
user_a	location_a	2018-01-01 09:00:00	60	2018-01-01 08:00:00	2	1	1
user_a	location_a	2018-01-01 11:00:00	60	2018-01-01 08:00:00	3	3	0
user_a	location_a	2018-01-01 12:00:00	60	2018-01-01 08:00:00	4	4	0
user_a	location_b	2018-01-01 10:00:00	60	2018-01-01 10:00:00	1	0	1
user_a	location_c	2018-01-01 08:00:00	60	2018-01-01 08:00:00	1	0	1
user_a	location_c	2018-01-01 09:00:00	60	2018-01-01 08:00:00	2	1	1
user_a	location_c	2018-01-01 10:00:00	60	2018-01-01 08:00:00	3	2	1
user_b	location_a	2018-01-01 15:00:00	60	2018-01-01 15:00:00	1	0	1
user_b	location_a	2018-01-01 16:00:00	60	2018-01-01 15:00:00	2	1	1
user_b	location_a	2018-01-01 18:00:00	60	2018-01-01 15:00:00	3	3	0
*/

# 完整 SQL 解决方案
select
    user_id,
    location_id,
    start_time,
    max(rn1 * 60) as duration
from
    (
        select
            user_id,
            location_id,
            u_dt,
            row_number() over w1      as rn1,
            first_value(u_dt) over w1 as start_time
        from
            (
                select
                    user_id,
                    location_id,
                    u_dt,
                    cast((row_number() over w) as signed) -
                    cast(TIMESTAMPDIFF(hour, (first_value(u_dt) over w), u_dt) as signed) gap
                from
                    user_location
                    window
                        w as (partition by user_id, location_id order by u_dt)
            ) t1
            window
                w1 as (partition by user_id, location_id, gap order by u_dt)
    ) t2
group by
    user_id,
    location_id,
    start_time
order by
    user_id,
    location_id,
    start_time;
/*
user_a	location_a	2018-01-01 08:00:00	120
user_a	location_a	2018-01-01 11:00:00	120
user_a	location_b	2018-01-01 10:00:00	60
user_a	location_c	2018-01-01 08:00:00	180
user_b	location_a	2018-01-01 15:00:00	120
user_b	location_a	2018-01-01 18:00:00	60
*/
