DROP TABLE IF EXISTS user_events;
DROP TABLE IF EXISTS agg_user_events;
DROP VIEW IF EXISTS mv_user_events;

CREATE TABLE user_events (
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime) 
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY DELETE;

CREATE TABLE agg_user_events (
    event_date Date,
    event_type String,
    uniq_user_state AggregateFunction(uniq, UInt32),
    sum_points_state AggregateFunction(sum, UInt32),
    count_event_state AggregateFunction(count, UInt32))
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW mv_user_events
TO agg_user_events
AS
SELECT toDate(event_time) AS event_date,
       event_type,
       uniqState(user_id) AS uniq_user_state,
       sumState(points_spent) AS sum_points_state,
       countState(event_type) AS count_event_state
FROM user_events
GROUP BY event_date, event_type;

INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());


INSERT INTO user_events VALUES
(7, 'signup', 0, now() - INTERVAL 14 DAY),
(8, 'signup', 0, now() - INTERVAL 14 DAY);

INSERT INTO user_events VALUES
(9, 'signup', 0, now() - INTERVAL 13 DAY),
(10, 'signup', 0, now() - INTERVAL 13 DAY);

-- Найдем первый день для каждого пользователя
WITH first_event AS	
    (SELECT user_id, toDate(MIN(event_time)) AS day_0
	 FROM user_events 
	 GROUP BY user_id),
-- К таблице с сырыми данными добавим столбец с первым днем активности для каждого пользователя	 
	 user_events_new AS   
	(SELECT user_id, event_type, points_spent, day_0, toDate(event_time) AS event_date
	 FROM user_events
	 LEFT JOIN first_event ON first_event.user_id = user_events.user_id) 
SELECT 
    ue_new.day_0,
    countDistinct(ue_new.user_id) AS total_users_day_0,
-- Считаем количество уникальных пользователей, которые вернулись в течение 7 дней
    countDistinctIf(ue_new.user_id,
                    ue_new.event_date > day_0 AND -- day_0 не включаем, начинаем со следующего дня
                    ue_new.event_date <= day_0 + 7
                   ) AS returned_in_7_days,
    round(100.0 * returned_in_7_days / total_users_day_0, 2) AS retention_7d_percent
FROM user_events_new AS ue_new
GROUP BY ue_new.day_0
ORDER BY ue_new.day_0;


SELECT mue.event_date,
       mue.event_type,
       uniqMerge(mue.uniq_user_state) AS unique_users,
       sumMerge(mue.sum_points_state) AS total_spent,
       countMerge(mue.count_event_state) AS total_actions
FROM mv_user_events mue 
GROUP BY mue.event_type, mue.event_date
ORDER BY mue.event_date, mue.event_type; 





