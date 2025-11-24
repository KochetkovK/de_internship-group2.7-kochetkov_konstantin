from pymongo import MongoClient
from pprint import pprint
from datetime import datetime, timedelta
import os
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27018/")
db = client["my_database"]
collection = db["user_events"]
collection_archiv = db["archived_users"]

today = datetime(2024, 2, 4, 12, 0, 0)  #datetime.now()
user_id_arch_list = []
count_ins_result = 0
count_rep_result = 0
file_name = f"4_4_work_with_mongo_db/{today.strftime('%Y-%m-%d')}.json"

query = {    
    "$and":
        [{"user_info.registration_date": {"$lt": today - timedelta(days = 30)}},
        {"event_time": {"$lt": today - timedelta(days = 14)}}
        ]
    }

data = collection.find(query)

if collection.find_one() is not None:
#Проверим, что бы в коллекции были данные
    for doc in data:
        if collection_archiv.find_one({"user_id": doc["user_id"]}) is None: #Проверим, что бы в архиве не было пользователя с тем же id
            collection_archiv.insert_one(doc)
            user_id_arch_list.append(doc["user_id"])
            count_ins_result += 1
        else: # Если пользователь уже в архиве, то заменим запись, если что то поменялось, проявил активность, а потом перестал заходить больше 14 дней
            if collection.find_one({"user_id": doc["user_id"]}) != collection_archiv.find_one({"user_id": doc["user_id"]}):
                collection_archiv.replace_one({"user_id": doc["user_id"]}, doc)
                user_id_arch_list.append(doc["user_id"])
                count_rep_result += 1

    result = {
        "date": today.strftime('%Y-%m-%d'),
        "archived_users_count": count_ins_result + count_rep_result,
        "arcived_user_ids": user_id_arch_list
    }
    
    if not os.path.exists(file_name):
        with open(file_name, 'w', encoding='utf-8') as res_file:
            json.dump(result, res_file, indent=2)
        print(f'✅Данные успешно записаны в файл, результат: {file_name}')
    else:
        print(f'❌Сегодня уже запускали этот скрипт, результат: {file_name}')

else:
    print(f'❌В коллекции "user_events" данных нет')


