select
    get_json_object(t.data, '$.delivery_type[0]') delivery_type_first_value
from
    (
        select
            '{
              "frequency": [
                {
                  "condition": [],
                  "range": [],
                  "strategy": [
                    {
                      "name": "mid",
                      "time_interval": 1,
                      "limit": 1
                    }
                  ]
                }
              ],
              "delivery_type": [
                1801
              ]
            }' data
    ) t;
-- 1801
