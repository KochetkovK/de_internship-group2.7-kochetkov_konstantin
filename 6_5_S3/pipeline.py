import asyncio
import os
import json
import logging
import pandas as pd
import re
from dotenv import load_dotenv
from pathlib import Path
from watchfiles import DefaultFilter, Change
from watchfiles import run_process, arun_process

from modules.async_client_s3 import AsyncObjectStorage
from modules.async_client_s3 import logger

path_log = Path('logs')
path_log.mkdir(exist_ok=True)
log_file = path_log / 'pipeline.log'

logger_pipeline = logging.getLogger(__name__)
logger_pipeline.setLevel(logging.INFO)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(log_file)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
logger_pipeline.addHandler(file_handler)
logger_pipeline.addHandler(stream_handler)

load_dotenv()

path_data = Path('data')
path_data.mkdir(exist_ok=True)
path_archive = Path('data/archive')
path_archive.mkdir(exist_ok=True)


class CustomFilter(DefaultFilter):
    allowed_name_file_pattern = r'.+[/\\]sales_202\d\-\d\d\-\d\d_\d\d:\d\d:\d\d\.csv'
    forbidden_path_pattern = r'.+[/\\]data[/\\]archive[/\\].+'
    def __call__(self, change: Change, path: str) -> bool:
        # Сначала применяем логику родительского класса (игнорирование .git и т.д.)
        # А затем добавляем свою проверку
        return (
            super().__call__(change, path) and
            (change == Change.added and  # Следит за созданием объекта, игнорирует изменение и удаление
            re.fullmatch(self.allowed_name_file_pattern, path) and  # Следим за появлением файла по нужному шаблону
            not re.fullmatch(self.forbidden_path_pattern, path)))   # Игнорируем изменения в архивной папке
            

def complex_func():
    async def handler_file():
        storage = AsyncObjectStorage(
            key_id= os.getenv("access_key"), # ВВЕДИТЕ_СВОЙ_access_key
            secret= os.getenv("secret"), # ВВЕДИТЕ_СВОЙ_secret_key
            endpoint="https://s3.ru-7.storage.selcloud.ru",
            container= "data-engineer-s3-practice" # ВВЕДИТЕ_НАЗВАНИЕ_КОНТЕЙНЕРА
        )
        need_file_pattern = fr"{path_data}[/\\]sales_202\d\-\d\d\-\d\d_\d\d:\d\d:\d\d\.csv"

        for input_file in path_data.glob('sales_*.csv'):
            if re.fullmatch(need_file_pattern, str(input_file)):
                df = pd.read_csv(input_file, encoding='utf-8')
                logger_pipeline.info(f"Файл {input_file} прочитан")
                try: 
                    df_new = df[((df['product'] == 'laptop') | (df['product'] == 'telephone'))]
                    logger_pipeline.info(f"Файл {input_file} отфильтрован")

                    filter_output_file = path_data.joinpath(f'{input_file.stem}_filter{input_file.suffix}')
                    df_new.to_csv(filter_output_file, index=False, encoding='utf-8')
                    logger_pipeline.info(f"Создан файл {filter_output_file}")


                    logger_pipeline.info('Отправляем файл в S3')
                    await storage.send_file(filter_output_file) # ЛОКАЛЬНЫЙ_ПУТЬ_ДО_ФАЙЛА)
                    
                    input_file.rename(path_archive.joinpath(input_file.name))
                    logger_pipeline.info(f"Перемещаем файл {input_file} в архив")

                    filter_output_file.unlink()
                    logger_pipeline.info(f"Временный файл {filter_output_file} удален")

                except KeyError as e:
                    logger_pipeline.info(f"Файл {input_file} не корректен, нет столбца: {e}")

                logger_pipeline.info("Обновляем лог файл в S3")
                await storage.send_file(log_file) 

    # Получаем информацию об изменениях из переменной окружения
    changes_str = os.getenv('WATCHFILES_CHANGES', '[]')
    changes = json.loads(changes_str)
    if changes and changes[0][0] == 'added' and len(changes) == 1:
        logger_pipeline.info('Добавился новый файл')
    elif changes and all(map(lambda x: x[0] == 'added', changes)) and len(changes) > 1:
        logger_pipeline.info('Добавились новые файлы')
    else:
        logger_pipeline.info("Первый запуск процесса...")
    asyncio.run(handler_file())   

async def main():
    await arun_process(str(path_data.resolve()), target=complex_func, args=(), watch_filter=CustomFilter())


if __name__ == '__main__':
    print("Главный процесс watchfiles запущен. Нажмите Ctrl+C для выхода.")
    logger_pipeline.info('Watchfiles запущен')
    asyncio.run(main())
    