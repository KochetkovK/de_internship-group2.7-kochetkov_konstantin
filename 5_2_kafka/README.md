# Задание
В базе данных PostgreSQL хранится таблица user_logins. В ней содержатся события пользователей, такие как логин, регистрация, покупка и т.д. Необходимо перенести эти события из PostgreSQL в другую систему (например, ClickHouse) с помощью Kafka. 

В результате должно получится устойчивое решение миграции данных с защитой от дубликатов.

# Решение

* Запустить контейнеры

   ``` docker-compose up -d```

* Если нет таблицы user_logins, запустить скрипт [insert_to_user_logins.py](insert_to_user_logins.py "Создает и запоняет таблицу user_logins")

* Запустить producer [producer_pg_to_kafka.py](producer_pg_to_kafka.py)

* Запустить consumer [consumer_to_clickhouse.py](consumer_to_clickhouse.py)

