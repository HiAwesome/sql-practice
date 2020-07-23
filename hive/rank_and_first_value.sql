select
    name,
    age,
    friends,
    rank() over (partition by name order by age desc) rank
from (
    select
        'zhangsan' name,
        18 age,
        10 friends
    UNION ALL
    select
        'zhangsan' name,
        19 age,
        20 friends
    UNION ALL
    select
        'zhangsan' name,
        20 age,
        30 friends
    UNION ALL
    select
        'zhangsan' name,
        40 age,
        100 friends
) t;
/*
zhangsan	40	100	1
zhangsan	20	30	2
zhangsan	19	20	3
zhangsan	18	10	4
*/

select
    name,
    friends
from (
     select
         name,
         friends,
         rank() over (partition by name order by age desc) rank
     from (
         select
             'zhangsan' name,
             18 age,
             10 friends
         UNION ALL
         select
             'zhangsan' name,
             19 age,
             20 friends
         UNION ALL
         select
             'zhangsan' name,
             20 age,
             30 friends
         UNION ALL
         select
             'zhangsan' name,
             40 age,
             100 friends
     ) t
) z
where
    z.rank = 1;
-- zhangsan	100

select
    name,
    age,
    friends,
    first_value(friends) over (partition by name order by age desc) first_value
from (
    select
        'zhangsan' name,
        18 age,
        10 friends
    UNION ALL
    select
        'zhangsan' name,
        19 age,
        20 friends
    UNION ALL
    select
        'zhangsan' name,
        20 age,
        30 friends
    UNION ALL
    select
        'zhangsan' name,
        40 age,
        100 friends
) t;
/*
zhangsan	40	100	100
zhangsan	20	30	100
zhangsan	19	20	100
zhangsan	18	10	100
*/

select
    distinct name,
    first_value(friends) over (partition by name order by age desc) first_value
from (
    select
        'zhangsan' name,
        18 age,
        10 friends
    UNION ALL
    select
        'zhangsan' name,
        19 age,
        20 friends
    UNION ALL
    select
        'zhangsan' name,
        20 age,
        30 friends
    UNION ALL
    select
        'zhangsan' name,
        40 age,
        100 friends
) t;
-- zhangsan	100

