# 3.3 按列位置排序
select
    prod_id,
    prod_price,
    prod_name
from
    Products
order by
    2, 3;
/*
BNBG02	3.49	Bird bean bag toy
BNBG01	3.49	Fish bean bag toy
BNBG03	3.49	Rabbit bean bag toy
RGAN01	4.99	Raggedy Ann
BR01	5.99	8 inch teddy bear
BR02	8.99	12 inch teddy bear
RYL01	9.49	King doll
RYL02	9.49	Queen doll
BR03	11.99	18 inch teddy bear
*/

# 5.3 not 操作符
select
    prod_name,
    prod_price
from
    Products
where
    not vend_id = 'DLL01'
order by
    prod_name;
/*
12 inch teddy bear	8.99
18 inch teddy bear	11.99
8 inch teddy bear	5.99
King doll	9.49
Queen doll	9.49
*/




