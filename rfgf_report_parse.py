import requests
from lxml import etree
import sys
from datetime import datetime
import pandas as pd
from multiprocessing import Pool


parser = etree.HTMLParser()
proxy = "http://192.168.3.1:3128"
proxyDict = {
    "http": proxy,
    "https": proxy,
    "ftp": proxy
}

url = "http://www.rfgf.ru/catalog/index.php"
r_data = {'authors': '', 'invn': '',
          'ftext': '', 'search': '0', 'docname': '', 'nom': '',
          'pnum': '', 'pdate': '', 'penddate': '', 'year': '',
          'place': '', 'full': '1', 'gg': '', 'mode': 'extctl',
          'orgisp': '', 'source': '', 'pi': ''}


def html_text_from_post(**kwargs):
    _r_data = r_data
    for key, data in kwargs.items():
        _r_data[key] = data
    r = requests.post(url, data=_r_data, proxies=proxyDict)
    text = r.text
    if text in ['Поиск не дал результатов. Попробуйте изменить параметры', None, '']:
        return None

    return text


def parse_rfgf(row):
    if row['invn'] != '':
        row['invn'] = int(row['invn'])
    text = html_text_from_post(
        authors=row['authors'], invn=row['invn'], pi=row['pi'])
    if text is not None:
        tree = etree.fromstring(text, parser)
        results = tree.xpath('//tr//td[position()=2]//a')
        urls = ['http://www.rfgf.ru/catalog/' +
                res.get('href').lstrip('./') for res in results]
        sys.stdout.write('{}) {}\n'.format(
            row['gkm_tgf'], ', '.join(urls)))
        row['urls'] = ', '.join(urls)

    return row


if __name__ == '__main__':
    start = datetime.now()
    df = pd.read_csv(
        'D://work//!Прогнозные ресурсы//Иркутская область//for_parser_test.csv', sep=';', encoding='cp1251')
    df.fillna('', inplace=True)
    with Pool(processes=4) as pool:
        rows = pool.map(parse_rfgf, df.to_dict('records'))
        # print(rows)
        ress = pd.DataFrame(rows)
        ress.to_csv('ress.csv', sep=';')
        print('\nParce complete')

    print(datetime.now() - start)
