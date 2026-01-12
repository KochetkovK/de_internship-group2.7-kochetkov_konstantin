import boto3
import os
import json
import time
import warnings
from urllib3.exceptions import InsecureRequestWarning
from dotenv import load_dotenv
from botocore.exceptions import ClientError 

load_dotenv()

with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=InsecureRequestWarning)

    session = boto3.Session(
        aws_access_key_id=os.getenv("access_key"),
        aws_secret_access_key=os.getenv("secret"),
    )
    
    s3_client = session.client('s3', endpoint_url='https://s3.ru-7.storage.selcloud.ru', verify=False)
    
    bucket_name = 'data-engineer-s3-practice'  
    
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
              "Sid": "PublicRead",
              "Effect": "Allow",
              "Principal": "*",
              "Action": "s3:GetObject",
              "Resource": f"arn:aws:s3:::{bucket_name}/*"
            },
            {
              "Sid": "AllowSpecificUserWrite",
              "Effect": "Allow",
              "Principal": {
                  "AWS": os.getenv('service_user_id')
              },
              "Action": "s3:PutObject",
              "Resource": f"arn:aws:s3:::{bucket_name}/*"
            }
        ]
    }
    bucket_policy = json.dumps(bucket_policy)
    
    s3_client.delete_bucket_policy(Bucket=bucket_name)
    # Добавим политику доступа
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)
    print(f'Добавлена политика доступа для бакета {bucket_name}')
    # Проверим политику доступа, загрузим и прочитаем файл
    s3_client.upload_file('data/test.txt', bucket_name, 'test.txt')
    
    response = s3_client.get_object(Bucket=bucket_name, Key='test.txt')
    data = response['Body'].read() 
    print(data)
    # Удалить файл не сможем
    try:
        s3_client.delete_object(Bucket=bucket_name, Key='test.txt')
    except ClientError as e:
        print('Доступ запрещен')
    
    # Удалим политику, что бы настроить версионирование
    s3_client.delete_bucket_policy(Bucket=bucket_name)
    # Включим версионирование
    s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})
    print(f'Версионирование включено для бакета {bucket_name}')
    
    
    for i in range(3):
        with open('data/test_version.txt', 'w') as f:
            f.write(f'File version: {i + 1}')
    
        s3_client.upload_file('data/test_version.txt', bucket_name, 'test_version.txt')
        print(f'Загружен файл test_version.txt версии {i + 1}')
        time.sleep(10)
    # Посмотрим все версии файла
    versions = s3_client.list_object_versions(Bucket=bucket_name)
    for version in versions.get('Versions', []):
        if version['Key'] == 'test_version.txt':
            print(f"Версия: {version['VersionId']} | Последний модифицированный: {version['LastModified']}")
    
    # Скачаем предпоследнию версию
    previous_version_id = [version['VersionId'] for version in versions.get('Versions', []) if version['Key'] == 'test_version.txt'][1]
    s3_client.download_file(Bucket=bucket_name, 
                            Key='test_version.txt', 
                            Filename='data/test_version_previous.txt',
                            ExtraArgs={'VersionId': previous_version_id})
    
    # Посмотрим скаченный файл
    with open('data/test_version_previous.txt') as f:
        print(f.read())

# В Selectel не поддерживается Bucket Lifecycle через boto3, сделал через интерфейс Selectel
#lifecycle_policy = {
#    'Rules': [
#        {
#            'ID': 'DeleteFilesAfter3Day',
#            'Status': 'Enabled',
#            'Filter': {'Prefix': ''},
#            'Expiration': {'Days': 3}
#        }
#    ]
#}

#s3_client.put_bucket_lifecycle_configuration(
#    Bucket=bucket_name,
#    LifecycleConfiguration=lifecycle_policy
#)
#policy_exists = s3_client.get_bucket_lifecycle_configuration(
#    Bucket=bucket_name)
#bucket_policy = policy_exists['Rules'][0]['Expiration']
