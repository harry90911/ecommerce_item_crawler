from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
from helpers.ProxyGenerator import ProxyGenerator
from threading import Lock
from helpers.Logger import create_logger
from api.api_helpers.momoCrawler_helpers import __generate_data, __generate_header
import os

logger = None
end_point = 'hide'
generator = ProxyGenerator(end_point)

def crawl_category(cat_dict: dict, generator: ProxyGenerator, crawl_list: list, output_path: str, today: str):

    listLock = Lock()
    fileLock = Lock()

    global logger
    headers = __generate_header()
    l3_id = cat_dict['id']
    data = __generate_data(l3_id)

    for i in range(30):
        proxy = generator.GetProxy()
        try:
            response = requests.post(end_point,
                            headers=headers, data=json.dumps(data).encode('utf-8'), proxies={'https':proxy}, timeout=10)
            categories = response.json()['rtnGoodsData']['categoryList']
            output = []
            for cat in categories:
                new_dict = cat_dict.copy()
                new_dict['id'] = cat['categoryCode']
                new_dict['content_name'] = cat['categoryName']
                if cat['action']['extraValue']['cateLevel'] == '2':
                    listLock.acquire()
                    crawl_list.append(new_dict)
                    listLock.release()
                
                # else:
                output.append(new_dict)

            if len(output) > 0:
                output = pd.DataFrame(output)

                fileLock.acquire()
                if os.path.exists(output_path+f'MomoCrawler_content_lst_{today}.csv'):
                    output.to_csv(output_path+f'MomoCrawler_content_lst_{today}.csv', header=False, mode='a', index=False)
                else:
                    output.to_csv(output_path+f'MomoCrawler_content_lst_{today}.csv', header=True, mode='w', index=False)
                fileLock.release()
            break
        except Exception as e:
            try:
                generator.HandleErrors(e, proxy)
            except Exception as e:
                logger.error(f'{cat_dict} threw error {e}')
                # return
    else:
        logger.error(f'Failed to download {cat_dict}')


def __content_crawler(output_path: str, today: str, log_dir: str):

    global logger
    logger = create_logger('momo content list crawler', log_dir=log_dir)
    logger.info('Initializing momo crawler content list')
    proxy = generator.GetProxy()
    cat_list = [{'cat_1_name':'3C','index':0},
                {'cat_1_name':'家電','index':1},
                {'cat_1_name':'美妝個清','index':2},
                {'cat_1_name':'保健/食品','index':3},
                {'cat_1_name':'服飾/內衣','index':4},
                {'cat_1_name':'鞋包/精品','index':5},
                {'cat_1_name':'母嬰用品','index':6},
                {'cat_1_name':'圖書文具','index':7},
                {'cat_1_name':'傢寢運動','index':8},
                {'cat_1_name':'日用生活','index':9},
                {'cat_1_name':'旅遊戶外','index':10}]
    while(True):
        try:
            page_source = requests.get('https://www.momoshop.com.tw/category/LgrpCategory.jsp?l_code=1199900000'
                                        , headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}
                                        , proxies={'https':proxy}
                                        , timeout=10).text
            soup = BeautifulSoup(page_source, 'lxml')

            cat_info_list = []
            for cat_l1 in cat_list:
                for cat_l2 in soup.findAll('table',{'class':'topmenu'})[cat_l1['index']].findAll('td'):
                    l2_name = cat_l2.find('p').text
                    for cat_l3 in cat_l2.findAll('li'):
                        cat_dict = {}
                        if cat_l3.a['href'].startswith('https://www.momoshop.com.tw') == True:
                            cat_dict['cat_l1_name'] = cat_l1['cat_1_name']
                            cat_dict['cat_l2_name'] = l2_name
                            cat_dict['cat_l3_name'] = cat_l3.a.text
                            cat_dict['l3_link'] = cat_l3.a['href'].split('&')[0]
                            try:
                                cat_dict['id'] = cat_dict['l3_link'].split('_code=')[1]
                            except IndexError:
                                continue
                            
                            cat_info_list.append(cat_dict)
            break
        except Exception as e:
            logger.error(e)
            proxy = generator.GetProxy()

    cat_dict = {}
    cat_dict['cat_l1_name'] = '日用生活'
    cat_dict['cat_l2_name'] = '日用/紙品'
    cat_dict['cat_l3_name'] = 'momo超市'
    cat_dict['l3_link'] = 'https://www.momoshop.com.tw/category/LgrpCategory.jsp?l_code=3900900000'
    cat_dict['id'] = cat_dict['l3_link'].split('_code=')[1]
    cat_info_list.append(cat_dict)

    generator.GetProxy() # Wait for initial proxy list to come in
    logger.info(f'Total categories count: {len(cat_info_list)}')
    while(len(cat_info_list) > 0):
        cat_dict = cat_info_list.pop()
        # crawl_category(cat_dict, generator, cat_info_list)
        generator.CrawlThread(target = crawl_category, args = [cat_dict, generator, cat_info_list, output_path, today])

        if (len(cat_info_list) == 0):
            generator.WaitAllCrawl()

    logger.info('End crawling momo crawler content list')