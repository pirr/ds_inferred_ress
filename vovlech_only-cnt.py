# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np


def rounder(val):
    if 0.000005 <= val < 0.00005:
        return(round(val, 5))
    elif 0.00005 <= val < 0.0005:
        return(round(val, 4))
    elif 0.0005 <= val < 0.005:
        return(round(val, 3))
    elif 0.005 <= val < 0.05:
        return(round(val, 2))
    elif 0.05 <= val <= 0.5:
        return(round(val, 1))
    else:
        return(round(val))



all_cols = {u'Название ПИ по ГБЗ': 'pi',
            u'Фонд недр \n(Р-распред., НР-нераспред.)': 'fund'}

# file_path = u'd://work//!Прогнозные ресурсы//АК,
# РА//reestr_ak-ra_gkm_05082016.xls'

csv_file = 'vovl_hak' + '.csv'
doc_file = 'vovl_hak_text.doc'
file_path = u'//192.168.3.36//PRS//СФО_2_кв_2017//Республика_Хакасия//reestr_rhk.xls'
# file_path = u'd://work//!Прогнозные ресурсы//Актуализация//2017//ИАМ//ирк//reestr_irk.xls'
data = pd.read_excel(file_path, sheetname='Реестр', skiprows=2)
data = data[(data[u'Актуальность строки'] == 'А')
            & ~(data[u'Вид объекта2)'].isin([u'РР', u'МЗ']))]
data[u'Название ПИ по ГБЗ'] = data[u'Название ПИ по ГБЗ'].str.capitalize()
# data = data[data[u'Субъект РФ']==u'Республика Алтай']
obj_pis_df = data[list(all_cols.keys()) + [u'№ объекта',
                                           u'Вид документа регистрации1)',
                                           u'Группа ПИ в госпрограмме3)',
                                           u'Наличие прогнозных ресурсов']]

opd = obj_pis_df.rename(columns=all_cols)

raspred_fund_id = opd.loc[opd['fund'] == u'Р',
                          u'№ объекта'].drop_duplicates().values.tolist()
if not raspred_fund_id:
    raise Exception('empty Licenses')

all_with_ress_id = opd.loc[opd[u'Наличие прогнозных ресурсов']
                           == u'да', u'№ объекта'].drop_duplicates().values.tolist()

raspred_fund_with_ress_id = opd.loc[opd[u'№ объекта'].isin(raspred_fund_id) & opd[
    u'№ объекта'].isin(all_with_ress_id), u'№ объекта'].drop_duplicates().values.tolist()
na_raspred_fund_with_ress_id = [
    x for x in all_with_ress_id if x not in raspred_fund_with_ress_id]


ids_list = [
    (u'распределенный', raspred_fund_id),
    (u'всего с ресурсами', all_with_ress_id),
    (u'нераспределенный с ресурсами', na_raspred_fund_with_ress_id),
    (u'распределенный с ресурсами', raspred_fund_with_ress_id),
]

merged_df = opd[['pi', u'Группа ПИ в госпрограмме3)']].drop_duplicates(subset=[
                                                                       'pi'])

df = opd.drop_duplicates(subset=['pi', u'№ объекта'])
df = df.groupby('pi').size().reset_index(name=u'Всего объектов')
merged_df = pd.merge(merged_df, df, on='pi', how='left')

for name, ids in ids_list:
    df = opd[opd[u'№ объекта'].isin(ids)].drop_duplicates(
        subset=['pi', u'№ объекта'])
    df = df.groupby('pi').size().reset_index(name=name)
    merged_df = pd.merge(merged_df, df, on='pi', how='left')

merged_df.fillna(0, inplace=True)

merged_df.insert(3, u'нераспределенный',
                 merged_df[u'Всего объектов'] - merged_df[u'распределенный'])

merged_df.insert(5, u'доля распределенного %',
                 (merged_df[u'распределенный'] / merged_df[u'Всего объектов']) * 100)

# merged_df.insert(8, u'доля нераспределенного с ресурсами %',
#                  (merged_df[u'нераспределенный с ресурсами'] / merged_df[u'Всего объектов']) * 100)

merged_df.insert(9, u'доля распределенного с ресурсами %',
                 (merged_df[u'распределенный с ресурсами'] / merged_df[u'всего с ресурсами']) * 100)

# merged_df.replace(np.inf, 100, inplace=True)
merged_df.replace(np.nan, 0, inplace=True)

perc_cols = [col for col in merged_df.columns if u'доля' in col]


for col in perc_cols:
    merged_df.loc[merged_df[col] == np.inf, col] = 100
    merged_df[col] = merged_df[col].apply(lambda val: rounder(val))

merged_df.replace(0, np.nan, inplace=True)
merged_df.sort_values(by='pi', inplace=True)

ress_df = merged_df.drop(u'Группа ПИ в госпрограмме3)', axis=1)

ress_df.to_csv(csv_file, sep=';', index=False)

# TEXT FOR REPORT

merged_df['text'] = np.nan
merged_df.loc[~pd.isnull(merged_df[u'распределенный']), 'text'] = merged_df[~pd.isnull(merged_df[u'распределенный'])].apply(
    lambda row: '- {}: {}%'.format(row['pi'], row[u'доля распределенного %']), axis=1)

text_list = merged_df.loc[~pd.isnull(
    merged_df['text']), 'text'].values.tolist()
text_1 = ';\n'.join(text_list) + '.'
text_1 = text_1.replace('.0%', '%')

_ress = []
for gr_pis in merged_df[pd.isnull(merged_df[u'распределенный'])].groupby(u'Группа ПИ в госпрограмме3)'):
    res = '- ' + ', '.join(pi.lower() for pi in gr_pis[1]['pi'].values)
    _ress.append(res)
text_2 = ';\n'.join(_ress) + '.'


with open(doc_file, 'w') as doc:
    if text_1:
        doc.write(text_1)
        doc.write('\n')
    doc.write(
        'Не вовлечены в недропользование прогнозные ресурсы объектов полезных ископаемых:\n')
    doc.write(text_2)
