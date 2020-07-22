CREATE TABLE generate_engine_table
(
    name  String,
    value UInt32
) ENGINE = GenerateRandom(1, 5, 3);


SELECT *
FROM
    generate_engine_table
LIMIT 10;
-- fSRH	2363536632
-- sX6>	1891556035
-- ""	1468485225


