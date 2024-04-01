from time import perf_counter
import pandas as pd
from bs4 import BeautifulSoup
import asyncio
import aiohttp

def read_parquet_file(file_path):
    df = pd.read_parquet(file_path)
    return df

async def fetch(session, url):
    try:
        async with session.get(f"https://{url}/contact", timeout=1000) as response:
            if response.status == 200:
                print(f"Pagina {url}/contact este disponibilă.")
                return f"https://{url}/contact"
            else:
                print(f"Pagina {url}/contact returnează codul de stare HTTP {response.status}.")
    except aiohttp.ClientError as e:
        print(f"Exceptie la {url}/contact", e)
        return None
async def fetch_all(session, urls):
    tasks = []
    for index, url in enumerate(urls, 1):
        tasks.append(fetch(session, url))
    results = await asyncio.gather(*tasks)
    return [url for url in results if url is not None]

def extract_addresses(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    addresses = []

    return addresses

async def main():
    start = perf_counter()
    parquet_file_path = "path//to//the//file"
    companyList = read_parquet_file(parquet_file_path)

    async with aiohttp.ClientSession() as session:
        urls_with_pages = await fetch_all(session, companyList["domain"])
        print("Linkurile cu pagini disponibile:", urls_with_pages)

    test = urls_with_pages
    print("Numarul de linkuri disponibile:", len(test))
    stop = perf_counter()
    print("time taken:", stop - start)

if __name__ == '__main__':
    asyncio.run(main())