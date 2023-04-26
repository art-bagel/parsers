import requests
import json
import os
from time import sleep
from typing import Any, Dict, List, Union

import pandas as pd


def get_catalogs_wb() -> List[dict]:
    """Берет категории и подкатегории из сохраненного файла, 
    если файла не существует запрашивает с сайта.

    Returns:
        List[dict]: категории и подкатегории
    """
    if not os.path.isfile('wb_catalogs_data.json'):
        url = 'https://www.wildberries.ru/webapi/menu/main-menu-ru-ru.json'
        headers = {'Accept': "*/*", 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(url, headers=headers).json()

        with open('wb_catalogs_data.json', 'w', encoding='UTF-8') as file:
            json.dump(response, file, indent=2, ensure_ascii=False)
        return response

    with open('wb_catalogs_data.json', 'r', encoding='UTF-8') as file:
        return json.loads(file.read())
    

def get_sub_catalog(sub_catalog: str, catalog: List[dict]) -> Union[List, dict]:
    """Возвращает данные подкатегории.

    Args:
        sub_catalog (str): искомый подкатегория
        data (List[dict]): данные с категориями

    Returns:
        Union[List, dict]: данные подкатегории
    """
    
    for sub_data in catalog:
        if sub_catalog in sub_data['url']:
            return sub_data
        

def get_metadata_for_catalog(url: str, catalog: List[Dict[str, Any]]) -> dict:
    """Излекает метаданные для подкатегории (название после крайнего "/" в url)

    Args:
        url (str): полный путь до категории
        catalog (List[Dict[str, Any]]): каталог с категориями/подкатегориями

    Returns:
        dict: метаданные подкатегории 
    """
    url = url.replace('https://www.wildberries.ru/catalog/', '')
    for name in url.split('/'):
        if 'childs' in catalog:
            catalog = get_sub_catalog(name, catalog['childs'])
        else:
            catalog = get_sub_catalog(name, catalog)
    return catalog


def get_content(shard: str, subject: str) -> List[dict]:
    """Запрашивает все товары размещаемые в целевой категории.

    Args:
        shard (str): внутреннее название категории WB
        subject (str): 

    Returns:
        List[dict]: данные товаров со всех страниц подкатегории
    """
    url = f'https://catalog.wb.ru/catalog/{shard}/catalog'
    headers = {'Accept': "*/*", 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    page = 1
    data = []
    while True:
        params = {
            'appType': '1', 
            'curr': 'rub', 
            'dest': '-1075831,-77677,-398551,12358499', 
            'locale': 'ru', 
            'page': page, 
            'reg': 0, 
            'regions': '64,83,4,38,80,33,70,82,86,30,69,1,48,22,66,31,40', 
            'sort': 'popular',
            'spp': 0,
            'subject': subject
        }
        response = requests.get(url, headers=headers, params=params).json()
        products = response['data']['products']
        data.extend(products)
        if not products: break
        page += 1
        sleep(1)
    return data 


def parse_content(data: List[dict]) -> List[dict]:
    """Преобразует сырые данные о товарах к нужному виду.

    Args:
        data (List[dict]): непреобразованные данные

    Returns:
        List[dict]: преобразованные данные
    """
    result = []
    for item in data:
        result.append({
            'Наименование': item['name'],
            'id': item['id'],
            'Скидка': item['sale'],
            'Цена/коп': int(item['priceU']),
            'Цена со скидкой/коп': int(item["salePriceU"]),
            'Бренд': item['brand'],
            'id бренда': int(item['brandId']),
            'feedbacks': item['feedbacks'],
            'rating': item['rating'],
            'Ссылка': f'https://www.wildberries.ru/catalog/{item["id"]}/detail.aspx?targetUrl=BP'
        })
    return result


if __name__ == '__main__':
    endpoint_data = 'https://www.wildberries.ru/catalog/elektronika/smartfony-i-telefony/vse-smartfony'
    wb_catalog = get_catalogs_wb()
    target_metadata = get_metadata_for_catalog(endpoint_data, wb_catalog)
    shard = target_metadata['shard']
    subject = target_metadata['query'].lstrip('subject=')
    raw_data = get_content(shard, subject)
    data = parse_content(raw_data)