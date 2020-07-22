CREATE TABLE id_val
(
    `id`  UInt32,
    `val` UInt32
) ENGINE = TinyLog;
INSERT INTO id_val
VALUES (1, 11)(2, 12)(3, 13);

CREATE TABLE id_val_join
(
    `id`  UInt32,
    `val` UInt8
) ENGINE = Join(ANY, LEFT, id);
INSERT INTO id_val_join
VALUES (1, 21)(1, 22)(3, 23);

-- [2020-06-30 16:34:47] Code: 264, e.displayText() = DB::Exception: Table default.id_val_join needs the same join_use_nulls setting as present in LEFT or FULL JOIN. (version 20.4.6.53 (official build))
SELECT *
FROM
    id_val ANY
        LEFT JOIN id_val_join
        USING (id) SETTINGS join_use_nulls = 1;

SELECT joinGet('id_val_join', 'val', toUInt32(1));
