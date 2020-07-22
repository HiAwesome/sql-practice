CREATE TABLE stripe_log_table
(
    timestamp    DateTime,
    message_type String,
    message      String
)
    ENGINE = StripeLog;


INSERT INTO stripe_log_table
VALUES (now(), 'REGULAR', 'The first regular message');
INSERT INTO stripe_log_table
VALUES (now(), 'REGULAR', 'The second regular message'),
       (now(), 'WARNING', 'The first warning message');

select *
from
    stripe_log_table;

SELECT *
FROM
    stripe_log_table
ORDER BY
    timestamp;
