# %%
import requests
import json
from api.api_helpers.momoCrawler_helpers import __generate_data, __generate_header
from helpers.ProxyGenerator import ProxyGenerator
from helpers.Logger import create_logger
from tqdm import tqdm
from threading import Lock
import pandas as pd
import os

logger = None
end_point = 'hide'
generator = ProxyGenerator(end_point)

def crawl_content(content, generator: ProxyGenerator, recrawl_list: list, pBar: tqdm, output_path: str, today: str):

    # today = get_local_date()
    recrawlLock = Lock()
    fileLock = Lock()

    # for skip in skip_list:
        # if skip in content.content_link:
            # pBar.update(1)
            # return
    global logger
    headers = __generate_header()

    try:
        data = __generate_data(str(content.id))
    except TypeError:
        pBar.update(1)
        return

    for i in range(10):
        proxy = generator.GetProxy()
        try:
            response = requests.post(end_point,
                            headers=headers, data=json.dumps(data).encode('utf-8'), proxies={'https':proxy}, timeout=10)
            break
        except Exception as e:
            try:
                generator.HandleErrors(e, proxy)
            except Exception as e:
                logger.error(f'{content} threw error {e}')
                recrawlLock.acquire()
                recrawl_list.append(content)
                recrawlLock.release()
                return
    else:
        logger.error(f'Too many retries on {content}')
        recrawlLock.acquire()
        recrawl_list.append(content)
        recrawlLock.release()
        return
        
    try:
        fileLock.acquire()

        brands = [brand.split('##') for brand in response.json()['rtnGoodsData']['brandName'][0]['brandNameStr']]
        brands = pd.DataFrame(brands, columns=['brand_name', 'brand_num'])
        brands['l1'] = content.cat_l1_name
        brands['l2'] = content.cat_l2_name
        brands['l3'] = content.cat_l3_name
        brands['l3_link'] = content.l3_link
        brands['content_id'] = content.id

        if os.path.exists(output_path+f'api_brand_{today}.csv'):
            brands.to_csv(output_path+f'api_brand_{today}.csv', header=False, mode='a', index=False)
        else:
            brands.to_csv(output_path+f'api_brand_{today}.csv', header=True, mode='w', index=False)
            
        pBar.update(1)
    except KeyError:
        pBar.update(1)
    except Exception as e:
        logger.error(f'{content} threw unexcepted error {e}')
    finally:
        fileLock.release()

def __api_brand_crawler(output_path: str, today: str, log_dir: str):

    global logger
    logger = create_logger('momo api brand crawler', log_dir=log_dir)

    contents = pd.read_csv(f'/home/hank/airflow/dags/raw_data/MomoCrawler_content_lst/MomoCrawler_content_lst_{today}.csv')
    contents = contents[['cat_l1_name', 'cat_l2_name', 'cat_l3_name', 'l3_link', 'id', 'content_name']]
    contents = [c for c in contents.drop_duplicates().itertuples()]

    skip_list = ['TOP', 'SALE', 'CPHOT', 'NEW']

    iteration = 0
    pBar = tqdm(total=len(contents))
    recrawl_list = []
    generator.GetProxy()
    while (len(contents) > 0 and iteration < 10):
        iteration += 1
        while (len(contents) > 0):
            content = contents.pop()
            generator.CrawlThread(target = crawl_content, args = [content, generator, recrawl_list, pBar, output_path, today])

        generator.WaitAllCrawl()

        contents = recrawl_list

    logger.info("Completed crawling")