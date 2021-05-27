import hashlib
import random
from os import path

lib_dir = path.join(path.dirname(__file__))

def __random_hash():
    return hashlib.sha1(str(random.getrandbits(128)).encode()).hexdigest()

def __generate_header():
    h = __random_hash()
    return {
        'device': '1',
        'Content-Type': 'application/json',
        'os': '13.5',
        'md': h,
        'User-Agent': f'[MOMOSHOP version:4.42.0;device:iOS;deviceID:{h};platform:1;userToken:;MOMOSHOP] showTB=0',
        'Accept-Language': 'en-CA;q=1, zh-Hant-TW;q=0.9',
        'version': '4.42.0',
        'pf': '1',
    }

def __generate_data(cateCode :int, brandCode :list = [], brandName :list = [], page :int = 1, catType :str = "" ):
    return {
        "data": {
            "prefere": "",
            "cateCode": f"{cateCode}",
            "sortType": "3",
            "isBrandPage": "0",
            "brandCode": brandCode,
            "brandName": brandName,
            "searchType":"6",
            "curPage": f"{page}",
            "cateType": catType
        }
    }