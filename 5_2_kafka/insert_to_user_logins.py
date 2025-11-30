import psycopg2
import time
import random

users = ["alice", "bob", "carol", "dave"]

with psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5435) as conn:
    with conn.cursor() as cursor:

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_logins (
            id SERIAL PRIMARY KEY,
            username TEXT,
            event_type TEXT,
            event_time TIMESTAMP
        )
        """)
        conn.commit()

        while True:
            data = {
                "user": random.choice(users),
                "event": "login",
                "timestamp": time.time()
            }

            cursor.execute("""
                INSERT INTO user_logins (
                    username,
                    event_type,
                    event_time)
                VALUES (%s, %s, to_timestamp(%s))""", tuple(data.values()))  
            conn.commit()  
        
            print(data)
            time.sleep(0.5)