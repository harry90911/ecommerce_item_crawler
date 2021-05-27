from api.momoCrawler_content import __content_crawler
from api.momoCrawler_brand import __api_brand_crawler
from api.momoCrawler_item import __item_crawler
from helpers.Datetimer import get_local_date

def content_crawler(output_path: str, today=get_local_date(), log_dir='./'):
    __content_crawler(output_path, today, log_dir)

def api_brand_crawler(output_path: str, today=get_local_date(), log_dir='./'):
    __api_brand_crawler(output_path, today, log_dir)

def item_crawler(output_path: str, cat_list_input: list, today=get_local_date(), log_dir='./'):
    __item_crawler(output_path, cat_list_input, today, log_dir)