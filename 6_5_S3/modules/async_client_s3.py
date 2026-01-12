import asyncio 
import os 
import logging
from dotenv import load_dotenv
from pathlib import Path

# Для создания асинхронного контекстного менеджера
from contextlib import asynccontextmanager
# Асинхронная версия boto3
from aiobotocore.session import get_session
# Ошибки при обращении к API
from botocore.exceptions import ClientError  

# Загружаем переменные окружения
load_dotenv()

# Создадим папку для логов, если ее нет 
path_log = Path('logs')
path_log.mkdir(exist_ok=True)

# Создадим логгер
logger = logging.getLogger(__name__)

class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
            "verify": False
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection

    async def send_file(self, local_source: str):
        """
        Загружает файл из локальной файловой системы в бакет S3
        """
        file_ref = Path(local_source)
        target_name = file_ref.name
        try:
            async with self._connect() as remote:
               with file_ref.open("rb") as binary_data:
                  await remote.put_object(
                      Bucket=self._bucket,
                      Key=target_name,
                      Body=binary_data
                  )    
            logger.info(f'File {target_name} uploaded in bucket {self._bucket}')
        except ClientError as e:
            logger.error(f'Failed to send {target_name}: {e}')


    async def fetch_file(self, remote_name: str, local_target: str):
        """
        Загружает файл из бакета S3 в локальную файловую систему
        """
        try:
            async with self._connect() as remote:
                response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
                body = await response["Body"].read()
                with open(local_target, "wb") as out:
                    out.write(body)    
            logger.info(f'File {remote_name} downloaded from bucket {self._bucket}. Path to file: {local_target}')
        except ClientError as e:
            logger.error(f'Failed to fetch {remote_name}: {e}')


    async def remove_file(self, remote_name: str):
        """
        Удаляет файл в бакете S3
        """
        try:
            async with self._connect() as remote:
                await remote.delete_object(Bucket=self._bucket, Key=remote_name)
    
            logger.info(f'File {remote_name} deleted from bucket {self._bucket}')
        except ClientError as e:
            logger.error(f'Failed to remove {remote_name}: {e}')


    async def list_files(self):
        """
        Возвращает список объектов в бакете S3
        """
        try:
            async with self._connect() as remote:
                response = await remote.list_objects_v2(Bucket=self._bucket)
                if not response:
                    logger.info(f'Bucket {self._bucket} empty')
                    return []
                lst_files = [obj["Key"] for obj in response["Contents"]]
                logger.info(f'List files in bucket {self._bucket}:')
                for f in lst_files:
                    logger.info(f' • {f}')
                return lst_files
        except ClientError as e:
            logger.error(f'Error listing files: {e}')


    async def file_exists(self, object_name):
        """
        Проверяет существование файла в бакете S3
        """
        async with self._connect() as remote:
            try:
                await remote.head_object(Bucket=self._bucket, Key=object_name)
                logger.info(f'File {object_name} exists in bucket {self._bucket}')
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.info(f'File {object_name} not exists in bucket {self._bucket}')
                    return False
                logger.error(f'Error checking file {object_name}: {e}')
                raise            


if __name__ == "__main__":
    # Настроим логгер для тестирования работы методов
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(path_log / 'result.log')
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    async def run_demo():
        """
        Функция для демонстрации работы методов
        """
        storage = AsyncObjectStorage(
            key_id= os.getenv("access_key"), # ВВЕДИТЕ_СВОЙ_access_key
            secret= os.getenv("secret"), # ВВЕДИТЕ_СВОЙ_secret_key
            endpoint="https://s3.ru-7.storage.selcloud.ru",
            container= "data-engineer-s3-practice" # ВВЕДИТЕ_НАЗВАНИЕ_КОНТЕЙНЕРА
        )   

        await storage.send_file("./data/test.txt") # ЛОКАЛЬНЫЙ_ПУТЬ_ДО_ФАЙЛА)
        await asyncio.sleep(5)
        await storage.fetch_file("test.txt", "data/get_test.txt") # (ИМЯ_ФАЙЛА_В_КОНТЕЙНЕРЕ, ПУТЬ_СОХРАНЕНИЯ_ЛОКАЛЬНО)
        await asyncio.sleep(5)
        await storage.list_files()
        await asyncio.sleep(5)
        await storage.file_exists("test.txt")
        await asyncio.sleep(5)
        await storage.remove_file("test.txt") # (ИМЯ_ФАЙЛА_В_КОНТЕЙНЕРЕ)
        await asyncio.sleep(5)
        await storage.file_exists("test.txt")
        await asyncio.sleep(5)
        await storage.send_file("./data/test.txt")

    asyncio.run(run_demo())        