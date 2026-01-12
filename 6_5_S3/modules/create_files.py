import asyncio
import time
import random
import pandas as pd
from datetime import date, datetime, timedelta

async def create_files(count_files: int = 5, count_line: int = 5):
    items = ['telephone', 'laptop', 'headphones', 'keyboard', 'mouse', 'apple', 'banana', 'bread', 'milk']
    for _ in range(count_files):
        await asyncio.sleep(random.uniform(1.5, 2.0))
        files_name = f"sales_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.csv"  
        data = []

        for _ in range(count_line):
            start_date = date(2025, 1, 1)
            end_date = date.today()
            random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
            data.append({'order_id': random.randint(1000, 100000),
                         'product': random.choice(items),
                         'amount': random.randint(10, 1000),
                         'date': random_date.isoformat()})
        df = pd.DataFrame(data)
        df.to_csv(f'data/{files_name}', index=False)

if __name__ == "__main__":

    async def main():
        while True:
            await create_files()
            await asyncio.sleep(2)
    
    asyncio.run(main())
    