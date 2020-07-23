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



select
    name,
    age,
    row_number() over () rn,
    ceil(row_number() over () / 3) row_3,
    ceil(row_number() over () / 5) row_5,
    ntile(3) over () n3,
    ntile(5) over () n5
from (
    select
        'lisi' name,
        18 age
    UNION ALL
    select
        'lisi' name,
        19 age
    UNION ALL
    select
        'lisi' name,
        20 age
    UNION ALL
    select
        'lisi' name,
        21 age
    UNION ALL
    select
        'lisi' name,
        18 age
    UNION ALL
    select
        'lisi' name,
        19 age
    UNION ALL
    select
        'lisi' name,
        20 age
    UNION ALL
    select
        'lisi' name,
        21 age
    UNION ALL
    select
        'lisi' name,
        18 age
    UNION ALL
    select
        'lisi' name,
        19 age
    UNION ALL
    select
        'lisi' name,
        20 age
    UNION ALL
    select
        'lisi' name,
        21 age
    UNION ALL
    select
        'lisi' name,
        18 age
    UNION ALL
    select
        'lisi' name,
        19 age
    UNION ALL
    select
        'lisi' name,
        20 age
) t;
-- lisi	20	1	1	1	1	1
-- lisi	19	2	1	1	1	1
-- lisi	18	3	1	1	1	1
-- lisi	21	4	2	1	1	2
-- lisi	20	5	2	1	1	2
-- lisi	19	6	2	2	2	2
-- lisi	18	7	3	2	2	3
-- lisi	21	8	3	2	2	3
-- lisi	20	9	3	2	2	3
-- lisi	19	10	4	2	2	4
-- lisi	18	11	4	3	3	4
-- lisi	21	12	4	3	3	4
-- lisi	20	13	5	3	3	5
-- lisi	19	14	5	3	3	5
-- lisi	18	15	5	3	3	5

