select
    name,
    age,
    row_num,
    round(row_num / 2) slice
from (
     select
         name,
         age,
         row_number() over (order by age) row_num
     from (
         select
             'zhangsan' name,
             18 age
         UNION ALL
         select
             'zhangsan' name,
             19 age
         UNION ALL
         select
             'zhangsan' name,
             20 age
         UNION ALL
         select
             'zhangsan' name,
             21 age
     ) t
) z;
-- zhangsan	18	1	1
-- zhangsan	19	2	1
-- zhangsan	20	3	2
-- zhangsan	21	4	2

select
    name,
    age,
    round(row_number() over (order by age) / 2) slice
from (
    select
        'zhangsan' name,
        18 age
    UNION ALL
    select
        'zhangsan' name,
        19 age
    UNION ALL
    select
        'zhangsan' name,
        20 age
    UNION ALL
    select
        'zhangsan' name,
        21 age
) t;
-- zhangsan	18	1
-- zhangsan	19	1
-- zhangsan	20	2
-- zhangsan	21	2


select
    name,
    age,
    row_number() over () row_num
from (
    select
        'zhangsan' name,
        18 age
    UNION ALL
    select
        'zhangsan' name,
        19 age
    UNION ALL
    select
        'zhangsan' name,
        20 age
    UNION ALL
    select
        'zhangsan' name,
        21 age
) t;


select
    name,
    age,
    row_num,
    ceil(row_num / 2) slice
from (
     select
         name,
         age,
         row_number() over (partition by name) row_num
     from (
         select
             'zhangsan' name,
             18 age
         UNION ALL
         select
             'zhangsan' name,
             19 age
         UNION ALL
         select
             'zhangsan' name,
             20 age
         UNION ALL
         select
             'zhangsan' name,
             21 age
     ) t
) z;

-- zhangsan,21,1,1
-- zhangsan,20,2,1
-- zhangsan,19,3,2
-- zhangsan,18,4,2


