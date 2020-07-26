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

SELECT
    user_id,
    location_id,
    u_dt,
    duration,
    lag(u_dt) OVER (PARTITION BY user_id, location_id ORDER BY u_dt) AS lag_dt,
    FROM_UNIXTIME(UNIX_TIMESTAMP(u_dt) - duration * 60)              as start_time
FROM
    user_location;


-- 第一步：开窗函数
SELECT
    user_id,
    location_id,
    UNIX_TIMESTAMP(u_dt) AS             u_dt,
    duration,
    row_number() OVER w  AS             r1,
    hour(u_dt) - hour(lag(u_dt) OVER w) gap_hour
FROM
    user_location
    window
        w as (PARTITION BY user_id, location_id ORDER BY u_dt);
/*
user_a	location_a	1514764800	60	1	8
user_a	location_a	1514768400	60	2	9	8
user_a	location_a	1514775600	60	3	11	9
user_a	location_a	1514779200	60	4	12	11
user_a	location_b	1514772000	60	1	10
user_a	location_c	1514764800	60	1	8
user_a	location_c	1514768400	60	2	9	8
user_a	location_c	1514772000	60	3	10	9
user_b	location_a	1514790000	60	1	15
user_b	location_a	1514793600	60	2	16	15
user_b	location_a	1514800800	60	3	18	16
*/

SELECT
    user_id,
    location_id,
    u_dt,
    from_unixtime(u_dt)                                           time_dt,
    duration,
    r1,
    gap_hour,
    from_unixtime(u_dt - (r1 * 3600 - 0 -
                          duration * 60) + ifnull((gap_hour - 1), 0) * 3600) start_time
FROM
    (SELECT
         user_id,
         location_id,
         UNIX_TIMESTAMP(u_dt) AS             u_dt,
         duration,
         row_number() OVER w  AS             r1,
         hour(u_dt) - hour(lag(u_dt) OVER w) gap_hour
     FROM
         user_location
         window
             w as (PARTITION BY user_id, location_id ORDER BY u_dt)
    ) AS temp1;
/*
user_a	location_a	1514764800	2018-01-01 08:00:00	60	1		2018-01-01 08:00:00
user_a	location_a	1514768400	2018-01-01 09:00:00	60	2	1	2018-01-01 08:00:00
user_a	location_a	1514775600	2018-01-01 11:00:00	60	3	2	2018-01-01 10:00:00
user_a	location_a	1514779200	2018-01-01 12:00:00	60	4	1	2018-01-01 09:00:00
user_a	location_b	1514772000	2018-01-01 10:00:00	60	1		2018-01-01 10:00:00
user_a	location_c	1514764800	2018-01-01 08:00:00	60	1		2018-01-01 08:00:00
user_a	location_c	1514768400	2018-01-01 09:00:00	60	2	1	2018-01-01 08:00:00
user_a	location_c	1514772000	2018-01-01 10:00:00	60	3	1	2018-01-01 08:00:00
user_b	location_a	1514790000	2018-01-01 15:00:00	60	1		2018-01-01 15:00:00
user_b	location_a	1514793600	2018-01-01 16:00:00	60	2	1	2018-01-01 15:00:00
user_b	location_a	1514800800	2018-01-01 18:00:00	60	3	2	2018-01-01 17:00:00
*/
