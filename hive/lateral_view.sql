select
    age
from (
    select
        '{"name": "zhangsan", "info": {"age":  18}}' data1
) t
LATERAL VIEW json_tuple(data1, 'info') t1 as info
LATERAL VIEW json_tuple(info, 'age') t2 as age;
-- 18
