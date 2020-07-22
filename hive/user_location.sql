-- 测试数据：
-- user_a    location_a    2018-01-01 08:00:00    60
-- user_a    location_a    2018-01-01 09:00:00    60
-- user_a    location_a    2018-01-01 11:00:00    60
-- user_a    location_a    2018-01-01 12:00:00    60
-- user_a    location_b    2018-01-01 10:00:00    60
-- user_a    location_c    2018-01-01 08:00:00    60
-- user_a    location_c    2018-01-01 09:00:00    60
-- user_a    location_c    2018-01-01 10:00:00    60
-- user_b    location_a    2018-01-01 15:00:00    60
-- user_b    location_a    2018-01-01 16:00:00    60
-- user_b    location_a    2018-01-01 18:00:00    60
-- 结果数据：
-- user_a    location_a    2018-01-01 08:00:00    120
-- user_a    location_a    2018-01-01 11:00:00    120
-- user_a    location_b    2018-01-01 10:00:00    60
-- user_a    location_c    2018-01-01 08:00:00    180
-- user_b    location_a    2018-01-01 15:00:00    120
-- user_b    location_a    2018-01-01 18:00:00    60


-- 建表
CREATE TABLE IF NOT EXISTS `user_location`
(
    user_id     string,
    location_id string,
    u_dt        string,
    duration    int
);

-- 插入数据
insert into table
    user_location
values
    ('user_a', 'location_a', '2018-01-01 08:00:00', 60),
    ('user_a', 'location_a', '2018-01-01 09:00:00', 60),
    ('user_a', 'location_a', '2018-01-01 11:00:00', 60),
    ('user_a', 'location_a', '2018-01-01 12:00:00', 60),
    ('user_a', 'location_b', '2018-01-01 10:00:00', 60),
    ('user_a', 'location_c', '2018-01-01 08:00:00', 60),
    ('user_a', 'location_c', '2018-01-01 09:00:00', 60),
    ('user_a', 'location_c', '2018-01-01 10:00:00', 60),
    ('user_b', 'location_a', '2018-01-01 15:00:00', 60),
    ('user_b', 'location_a', '2018-01-01 16:00:00', 60),
    ('user_b', 'location_a', '2018-01-01 18:00:00', 60);

-- 第一步：开窗函数
SELECT
    user_id,
    location_id,
    UNIX_TIMESTAMP(u_dt)                                                AS u_dt,
    duration,
    row_number() OVER (PARTITION BY user_id, location_id ORDER BY u_dt) AS row_number
FROM
    user_location;
-- user_a	location_a	1514764800	60	1
-- user_a	location_a	1514768400	60	2
-- user_a	location_a	1514775600	60	3
-- user_a	location_a	1514779200	60	4
-- user_a	location_b	1514772000	60	1
-- user_a	location_c	1514764800	60	1
-- user_a	location_c	1514768400	60	2
-- user_a	location_c	1514772000	60	3
-- user_b	location_a	1514790000	60	1
-- user_b	location_a	1514793600	60	2
-- user_b	location_a	1514800800	60	3

-- 第二步：获取每条记录的开始时间
SELECT
    user_id,
    location_id,
    u_dt,
    duration,
    row_number,
    u_dt - (row_number * 3600 - duration * 60) AS start_time
FROM
    (SELECT
         user_id,
         location_id,
         UNIX_TIMESTAMP(u_dt)                                                AS u_dt,
         duration,
         row_number() OVER (PARTITION BY user_id, location_id ORDER BY u_dt) AS row_number
     FROM
         user_location) AS temp1;
-- user_a	location_a	1514764800	60	1	1514764800
-- user_a	location_a	1514768400	60	2	1514764800
-- user_a	location_a	1514775600	60	3	1514768400
-- user_a	location_a	1514779200	60	4	1514768400
-- user_a	location_b	1514772000	60	1	1514772000
-- user_a	location_c	1514764800	60	1	1514764800
-- user_a	location_c	1514768400	60	2	1514764800
-- user_a	location_c	1514772000	60	3	1514764800
-- user_b	location_a	1514790000	60	1	1514790000
-- user_b	location_a	1514793600	60	2	1514790000
-- user_b	location_a	1514800800	60	3	1514793600

-- 第三步：分组获取各自数量
SELECT
    user_id,
    location_id,
    start_time,
    count(start_time) AS cnt
FROM
    (SELECT
         user_id,
         location_id,
         u_dt,
         duration,
         row_number,
         u_dt - (row_number * 3600 - duration * 60) AS start_time
     FROM
         (SELECT
              user_id,
              location_id,
              UNIX_TIMESTAMP(u_dt)                                                AS u_dt,
              duration,
              row_number() OVER (PARTITION BY user_id, location_id ORDER BY u_dt) AS row_number
          FROM
              user_location) AS temp1) AS temp2
GROUP BY
    user_id,
    location_id,
    start_time;
-- user_id	location_id	start_time	cnt
-- user_a	location_a	1514764800	2
-- user_a	location_a	1514768400	2
-- user_a	location_b	1514772000	1
-- user_a	location_c	1514764800	3
-- user_b	location_a	1514790000	2
-- user_b	location_a	1514793600	1

-- 最终 SQL:
SELECT
    user_id,
    location_id,
    FROM_UNIXTIME(start_time, 'yyyy-MM-dd HH:mm:ss') AS sequence_dt,
    cnt * 60                                         AS sequence_duration
FROM
    (SELECT
         user_id,
         location_id,
         start_time,
         count(start_time) AS cnt
     FROM
         (SELECT
              user_id,
              location_id,
              u_dt,
              duration,
              row_number,
              u_dt - (row_number * 3600 - duration * 60) AS start_time
          FROM
              (SELECT
                   user_id,
                   location_id,
                   UNIX_TIMESTAMP(u_dt)                                                AS u_dt,
                   duration,
                   row_number() OVER (PARTITION BY user_id, location_id ORDER BY u_dt) AS row_number
               FROM
                   user_location) AS temp1) AS temp2
     GROUP BY
         user_id,
         location_id,
         start_time) AS temp3;
-- user_a	location_a	2018-01-01 08:00:00	120
-- user_a	location_a	2018-01-01 09:00:00	120
-- user_a	location_b	2018-01-01 10:00:00	60
-- user_a	location_c	2018-01-01 08:00:00	180
-- user_b	location_a	2018-01-01 15:00:00	120
-- user_b	location_a	2018-01-01 16:00:00	60
