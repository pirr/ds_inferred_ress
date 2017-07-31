import pandas as pd
import numpy as np
import re

report_cols = ['Номер ТГФ', 'Номер РГФ', 'ИД',
               'Код документа', 'Автор', 'Год утверждения',
               'Название документа', 'Вид документа', 'Инвентарный номер документа']

doc_cols = ['ИД объекта ГКМ', 'Вышестоящее геогр образование', 'Название месторождения', 'Название',
            'Массив ГКМ', 'Номер паспорта ТГФ']

sec_name_pat = re.compile(r'[А-Я]\w+')


def gkmdf_items(gkmdf, args):
    return gkmdf.groupby([args[0]])[args]\
        .transform(lambda grp: grp.fillna(method='ffill')).\
        drop_duplicates(subset=args[0])

reports_from_nedra = pd.read_excel('reports_irk.xls', header=7)
reports_from_nedra = reports_from_nedra[1:]
cols = ['ИД объекта ГКМ'] + \
    [c for c in reports_from_nedra.columns if c not in ['ИД объекта ГКМ']]
reports_from_nedra['ИД объекта ГКМ'].fillna(method='pad', inplace=True)

# reports_from_nedra[doc_cols] = gkmdf_items(reports_from_nedra, doc_cols)
reports_from_nedra_doc = gkmdf_items(reports_from_nedra, doc_cols)
reports_from_nedra = pd.merge(reports_from_nedra[
                              report_cols + ['ИД объекта ГКМ']], reports_from_nedra_doc, how='outer', on='ИД объекта ГКМ')
reports_from_nedra['gkm_tgf'] = reports_from_nedra.apply(
    lambda x: '-'.join([x[u'Массив ГКМ'], str(int(x[u'Номер паспорта ТГФ']))]), axis=1)
reports_from_nedra['authors'] = reports_from_nedra[
    'Автор'].str.findall(sec_name_pat)
reports_from_nedra['invn'] = reports_from_nedra['Номер ТГФ']

reports_for_scan = pd.read_excel(
    'Отчеты_для_сканирования_rab.xls', sheetname='Лист1', header=1)
reports_for_scan = reports_for_scan[1:]

# reports_for_scan['authors'] = reports_for_scan['author'].str.findall(sec_name_pat)
# reports_for_scan['authors'] = reports_for_scan['authors'].apply(lambda x: ','.join(x) if x else '')
reports_from_nedra_scan = reports_from_nedra[
    reports_from_nedra['gkm_tgf'].isin(reports_for_scan['obj_number'])]
reports_from_nedra_scan.dropna(subset=['gkm_tgf', 'authors'], inplace=True)
reports_from_nedra_scan['authors'] = reports_from_nedra_scan[
    'authors'].apply(lambda x: ','.join(x) if '' not in x else '')
reports_from_nedra_scan.to_csv('for_parser_test_19012017.csv', sep=';')
# reports_for_scan.loc[~pd.isnull(reports_for_scan['author_sec_name']), 'author_sec_name'] = reports_for_scan['author_sec_name'].apply(lambda x: set(x))
# reports_for_scan_with_tgf_num = pd.merge(reports_for_scan, reports_from_nedra, how='outer',
#                                          left_on=['obj_number', 'author'], right_on=['gkm_tgf', 'Автор'])
# reports_for_scan_with_tgf_num.to_csv('for_parser.csv', sep=';')
