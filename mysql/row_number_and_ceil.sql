select
    name,
    age,
    row_num,
    ceil(row_num / 2) slice
from
    (
        select
            name,
            age,
            row_number() over (order by name) row_num
        from
            (
                select
                    'zhangsan' name,
                    18         age
                UNION ALL
                select
                    'zhangsan' name,
                    19         age
                UNION ALL
                select
                    'zhangsan' name,
                    20         age
                UNION ALL
                select
                    'zhangsan' name,
                    21         age
            ) t
    ) z;
# zhangsan,18,1,1
# zhangsan,19,2,1
# zhangsan,20,3,2
# zhangsan,21,4,2

select
    name,
    age,
    ceil(row_number() over () / 3) slice
from
    (
        select
            'zhangsan' name,
            18         age
        UNION ALL
        select
            'zhangsan' name,
            19         age
        UNION ALL
        select
            'zhangsan' name,
            20         age
        UNION ALL
        select
            'zhangsan' name,
            21         age
    ) t;
# zhangsan	18	1
# zhangsan	19	1
# zhangsan	20	1
# zhangsan	21	2

select
    name,
    age,
    row_number() over () row_num
from
    (
        select
            'zhangsan' name,
            18         age
        UNION ALL
        select
            'zhangsan' name,
            19         age
        UNION ALL
        select
            'zhangsan' name,
            20         age
        UNION ALL
        select
            'zhangsan' name,
            21         age
    ) t;
# zhangsan	18	1
# zhangsan	19	2
# zhangsan	20	3
# zhangsan	21	4


select
    name,
    age,
    row_number() over (order by age)      row_num_age,
    row_number() over (order by name)     row_num_name,
    row_number() over (partition by name) row_num_pa_name,
    row_number() over ()                  row_num
from
    (
        select
            'zhangsan' name,
            18         age
        UNION ALL
        select
            'zhangsan' name,
            19         age
        UNION ALL
        select
            'lisi' name,
            20     age
        UNION ALL
        select
            'zhangsan' name,
            21         age
        UNION ALL
        select
            'zhangsan' name,
            22         age
    ) t;
# lisi,20,3,1,1,1
# zhangsan,18,1,2,1,2
# zhangsan,19,2,3,2,3
# zhangsan,21,4,4,3,4
# zhangsan,22,5,5,4,5