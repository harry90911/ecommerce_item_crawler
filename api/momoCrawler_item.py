import requests
import json
import pandas as pd
from helpers.ProxyGenerator import ProxyGenerator
from threading import Lock
from helpers.Datetimer import get_local_date
from helpers.Logger import create_logger
from api.api_helpers.momoCrawler_helpers import __generate_data, __generate_header
from tqdm import tqdm
import os

logger = None
end_point = 'hide'
generator = ProxyGenerator(end_point)

def crawl_page(brand, generator : ProxyGenerator, brand_list : list, recrawl_list : list, pBar : tqdm, output_path: str, today: str):

    listLock = Lock()
    recrawlLock = Lock()
    fileLock = Lock()

    global logger
    headers = __generate_header()
    data = __generate_data(brand.id, [str(brand.brandCode)], [brand.brandName], brand.page)
    
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
                logger.error(f'{brand} threw error {e}')
                recrawlLock.acquire()
                recrawl_list.append(brand)
                recrawlLock.release()
                return
    else:
        logger.error(f'Too many retries on {brand}')
        recrawlLock.acquire()
        recrawl_list.append(brand)
        recrawlLock.release()
        return
        
    try: 
        js = response.json()
        max_page = int(js['maxPage'])

        if brand.first and max_page != 1:
            new_list = []
            for i in range(brand.page + 1, max_page + 1):
                new_list.append(brand._replace(page = i, first = False))

            listLock.acquire()
            brand_list.extend(new_list)
            pBar.total += len(new_list)
            pBar.refresh()
            listLock.release()
            logger.info(f'Addings {max_page - 1} pages to crawl for {brand.brandName} in {brand.l3}, new total {pBar.total}')
        
        goods = js['rtnGoodsData']['goodsInfoList']
        goodsList = []
        for i, good in enumerate(goods):
            g = {
                'l1': brand.l1,
                'l2': brand.l2,
                'l3': brand.l3,
                'l3_id' : brand.id,
                'brand' : brand.brandName,
                'brand_num' : brand.brandCode,
                'imgUrl' : good['imgUrl'],
                'imgTagUrl' : good['imgTagUrl'],
                'useCounpon' : good['useCounpon'],
                'isDiscount' : good['isDiscount'],
                'haveGift' : good['haveGift'],
                'item_name' : good['goodsName'],
                'special_title' : good['goodsSubName'],
                'item_id' : good['goodsCode'],
                'price' : good['SALE_PRICE'],
                'stock' : good['goodsStock'],
                'page' : brand.page,
                'max_page' : max_page,
                'rank' : i
            }
            if ('icon' in good.keys()):
                g['icons'] = ','.join(i['iconContent'] for i in good['icon'])
            else:
                g['icons'] = ''
            goodsList.append(g)
        fileLock.acquire()
        try:
            if os.path.exists(output_path+f'api_items_{today}.csv'):
                pd.DataFrame(goodsList).to_csv(output_path+f'api_items_{today}.csv', header=False, mode='a', index=False)
            else:
                pd.DataFrame(goodsList).to_csv(output_path+f'api_items_{today}.csv', header=True, mode='w', index=False)
        except Exception as e:
            logger.error(f'{brand} threw unexcepted error {e}')
        pBar.update(1)
        fileLock.release()

        if (brand.page == max_page and int(js["totalCnt"]) > 100):
            logger.info(f'Crawled last page for {brand} with {js["totalCnt"]} items')

    except Exception as e:
        logger.error(f'{brand} threw unexcepted error {e}')
        recrawlLock.acquire()
        recrawl_list.append(brand)
        recrawlLock.release()

def __item_crawler(output_path: str, cat_list_input:list, today: str, log_dir: str):
    
    global logger
    logger = create_logger('momo api brand crawler', log_dir=log_dir)

    if os.path.exists(f'{output_path}/api_brand_{today}.csv'):
        content = pd.read_csv(f'{output_path}/api_brand_{today}.csv')
    elif os.path.exists(f'{output_path}/api_brand_{get_local_date(-1)}.csv'):
        content = pd.read_csv(f'{output_path}/api_brand_{get_local_date(-1)}.csv')
    elif os.path.exists(f'{output_path}/api_brand_{today}.zip'):
        content = pd.read_csv(f'{output_path}/api_brand_{today}.zip')
    else:
        content = pd.read_csv(f'{output_path}/api_brand_{today}.csv')

    content = content.drop_duplicates(['l1', 'l2', 'l3', 'brand_num'])
    content['cat_l3_id'] = content['l3_link'].str.split('_code=').str[1]
    content = content[~content['cat_l3_id'].isna()]
    content['cat_l3_id'] = content['cat_l3_id'].astype(int)
    content['brand_num'] = content['brand_num'].astype(int)
    content.rename({
        'cat_l3_id': 'id',
        'brand_name': 'brandName',
        'brand_num': 'brandCode'
        }, inplace=True, axis=1)
    content['page'] = 1
    content['first'] = True

    if (len(cat_list_input) > 0):
        content = content[content['l1'].isin(cat_list_input)]

    brand_list = list(content.itertuples(index=False))

    if os.path.exists(output_path+f'api_items_{today}.csv'):
        processed = pd.read_csv(output_path+f'api_items_{today}.csv')
        incomplete = processed.sort_values('page', ascending=False)
        incomplete = incomplete.groupby(['l3_id', 'brand_num']).head(1)
        incomplete = incomplete[incomplete['page'] != incomplete['max_page']]
        incomplete = {(l,b): p for l, b, p in zip(incomplete['l3_id'], incomplete['brand_num'], incomplete['page'])}
        processed = processed[['l3_id', 'brand_num']]
        processed = {(l,b) for l, b in zip(processed['l3_id'], processed['brand_num'])}
        unprocessed = []
        for brand in brand_list:
            if (brand.id, brand.brandCode) in processed:
                if (brand.id, brand.brandCode) in incomplete:
                    unprocessed.append(brand._replace(page=incomplete[(brand.id, brand.brandCode)]))
                continue
            unprocessed.append(brand)
        brand_list = unprocessed

    generator.GetProxy()

    iteration = 0
    while(len(brand_list) > 0 and iteration < 6):
        logger.info(f'Starting iteration {iteration + 1} with {len(brand_list)} items')

        recrawl_list = []
        pBar = tqdm(total=len(brand_list))
        count = 0
        while(len(brand_list) > 0):
            brand = brand_list.pop()
            # crawl_page(brand, generator, brand_list, recrawl_list, pBar)
            generator.CrawlThread(target = crawl_page, args = [brand, generator, brand_list, recrawl_list, pBar, output_path, today])

        generator.WaitAllCrawl()

        brand_list = recrawl_list
        iteration += 1

    logger.info("Finished crawling items")