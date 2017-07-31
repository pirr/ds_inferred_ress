# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np


res_cols = ['pi', 'fund', 'unit',
            'P3', 'P3_cnt',
            'P2', 'P2_cnt',
            'P1', 'P1_cnt',
            'non_cat', 'non_cat_cnt',
            'summ',
            'C2', 'C2_cnt']
pi_cols = {u'Название ПИ по ГБЗ': 'pi', u'Ед. измерения ПИ': 'unit'}
cat_cols = {u'P3': 'P3', u'P2': 'P2', u'P1': 'P1',
            u'Без категор.': 'non_cat', u'С2': 'C2'}
all_cols = pi_cols.copy()
all_cols.update(cat_cols)

# file_path = u'd://work//!Прогнозные ресурсы//АК,
# РА//reestr_ak-ra_gkm_05082016.xls'
file_path = u'//192.168.3.36//PRS//WORK//Marina//8 субъектов//Готовые реестры//reestr_bur.xls'
data = pd.read_excel(file_path, sheetname='Реестр', skiprows=2)
data = data[(data[u'Актуальность строки'] == 'А')
            & (data[u'Вид объекта2)'] != u'КТ')]
data[u'Название ПИ по ГБЗ'] = data[u'Название ПИ по ГБЗ'].str.capitalize()
# data = data[data[u'Субъект РФ']==u'Республика Алтай']
obj_pis_df = data[list(all_cols.keys()) + [u'№ объекта',
                                           u'Вид документа регистрации1)',
                                           u'Группа ПИ в госпрограмме3)']]

opd = obj_pis_df.rename(columns=all_cols)
_opd = opd.dropna(
    subset=[cat for cat in cat_cols.values() if cat != 'C2'], how='all')


def get_all_and_lic_groups_by_obj(df):

    def _grouping(df):
        return df.groupby(['pi', 'unit', u'№ объекта']).max().reset_index()
    all_groups = _grouping(df)
    lic_groups = _grouping(
        df[df[u'Вид документа регистрации1)'] == u'Лицензии'])

    # all_groups = all_groups.set_index(['pi', 'unit'])
    # lic_groups = lic_groups.set_index(['pi', 'unit'])
    all_groups = all_groups[all_groups['pi'].isin(lic_groups['pi'].unique())]
    
    # all_groups = pd.concat(_all_groups, all_groups,
    #                       left_index=True, right_index=True)
    return all_groups, lic_groups


def get_group_by_pi_sum_count(df):

    df = df.drop([u'№ объекта', u'Вид документа регистрации1)', u'Группа ПИ в госпрограмме3)'], axis=1)
    df_sum = df.fillna(0)
    group_by_pi_sum = df_sum.groupby(['pi', 'unit'])
    group_by_pi_sum = group_by_pi_sum.sum()
    group_by_pi_sum['summ'] = group_by_pi_sum.sum(axis=1)
    group_by_pi_cnt = df.groupby(['pi', 'unit'])
    group_by_pi_count = group_by_pi_cnt.count()
    group_by_pi_count.columns = [c + '_cnt' for c in group_by_pi_count.columns if c != 'fund']
    
    group_by_pi_sum_count = pd.concat(
        [group_by_pi_sum, group_by_pi_count], axis=1)
    group_by_pi_sum_count.replace(0, np.nan, inplace=True)

    return group_by_pi_sum_count


all_groups, lic_groups = get_all_and_lic_groups_by_obj(_opd)


if lic_groups.empty:
    raise Exception('empty Licenses')

all_group_by_pi_sum_count = get_group_by_pi_sum_count(all_groups)
all_group_by_pi_sum_count['fund'] = u'всего'

lic_group_by_pi_sum_count = get_group_by_pi_sum_count(lic_groups)
lic_group_by_pi_sum_count['fund'] = u'в т. ч. распред.'

lic_part = (lic_group_by_pi_sum_count.drop(
    'fund', axis=1) / all_group_by_pi_sum_count.drop('fund', axis=1)) * 100
lic_part['fund'] = u'доля распред. %'
lic_group_by_pi_sum_count.to_csv("test_lic_gr.csv", sep=";")

ress = pd.concat([all_group_by_pi_sum_count,
                  lic_group_by_pi_sum_count, lic_part]).reset_index()

ress['fund'] = pd.Categorical(
    ress['fund'], [u"всего", u"в т. ч. распред.", u"доля распред. %"])
ress = ress.sort_values(['pi', 'fund', 'unit'])

ress[res_cols].replace(0, np.nan).to_csv('actual_bur_vovl.csv', sep=';')


# TEXT FOR REPORT
text_1 = None
arr_text = ress.loc[ress['fund'] == u"доля распред. %",
                    ['pi', 'P3_cnt', 'P2_cnt', 'P1_cnt', 'non_cat_cnt']].fillna(0).values
_ress = []
for t in arr_text:
    res = zip(['P3', 'P2', 'P1', 'Без категор.'], t[1:])
    res = ', '.join(' - '.join((k, str(v)+'%'))
                    for k, v in res if v)
    res = ': '.join(['- ' + t[0], res])
    _ress.append(res)
text_1 = ';\n'.join(_ress) + '.'
text_1 = text_1.replace('.0%', '%')

no_vovl = opd.loc[~opd['pi'].isin(
    ress['pi']), ['pi', u'Группа ПИ в госпрограмме3)']].drop_duplicates('pi')

_ress = []
for gr_pis in no_vovl.groupby(u'Группа ПИ в госпрограмме3)'):
    res = '- ' + ', '.join(pi.lower() for pi in gr_pis[1]['pi'].values)
    _ress.append(res)
# for pis in arr_text:
#     res = '- ' + ', '.join(pi.lower() for pi in pis)
#     _ress.append(res)
text_2 = ';\n'.join(_ress) + '.'

with open('in_actual_bur_rep.doc', 'w') as doc:
    if text_1:
        doc.write(text_1)
        doc.write('\n')
    doc.write(
        'Не вовлечены в недропользование прогнозные ресурсы объектов полезных ископаемых:\n')
    doc.write(text_2)

# test_all_count_by_pi = test_all_group.groupby(['pi', 'unit']).count()
# [list(all_cols.values())].max()

# test_lic[list(all_cols.values())].max().to_csv('test_vovl.csv', sep=';')


# def get_obj_pis_group(df, pi_cols):
#     if type(pi_cols) is not list:
#         pi_cols = [pi_cols]
#     obj_pis_df_withress = df.groupby(
#         pi_cols + [u'№ объекта', ]).max().reset_index()

#     if obj_pis_df_withress.empty:
#         return False

#     obj_pis_df_withress = obj_pis_df_withress.drop(u'№ объекта', axis=1)
#     obj_pis_df_withress = obj_pis_df_withress.drop(
#         u'Вид документа регистрации1)', axis=1)
#     obj_pis_df_withress = obj_pis_df_withress.groupby(pi_cols)
#     return obj_pis_df_withress


# def get_total_val_by_cols(df, val_name='val'):
#     total_val_name = 'total_' + val_name
#     val_cols = {c: val_name + ' ' + c for c in cat_cols.values()}
#     inf_cols_val = [c for c in val_cols.values() if 'C2' not in c]

#     if val_name == 'val':
#         obj_pis_df_withress_val = df.sum()
#     elif val_name == 'cnt':
#         obj_pis_df_withress_val = df.count()

#     obj_pis_df_withress_val = obj_pis_df_withress_val.rename(columns=val_cols)
#     obj_pis_df_withress_val[total_val_name] = obj_pis_df_withress_val[
#         inf_cols_val].sum(axis=1)
#     return obj_pis_df_withress_val


# def get_obj_by_pi_count(df, fund=u'всего'):
#     obj_by_pi = get_obj_pis_group(df, 'pi')
#     if obj_by_pi:
#         obj_by_pi = obj_by_pi.size().to_frame().rename(
#             columns={0: 'total_cnt'})
#         obj_by_pi['fund'] = fund
#         return obj_by_pi.reset_index()
#     return pd.DataFrame()


# def get_ress_df_concat(df, pi_cols, fund=u'фонд'):
#     obj_pis_df_withress = get_obj_pis_group(df, pi_cols)

#     if not obj_pis_df_withress:
#         print(u'Empty dataframe in {}!'.format(fund))
#         return pd.DataFrame(columns=all_cols)

#     obj_pis_df_withress_val = get_total_val_by_cols(obj_pis_df_withress, 'val')
#     obj_pis_df_withress_count = get_total_val_by_cols(
#         obj_pis_df_withress, 'cnt')
#     obj_pis_df_withress_concat = pd.concat(
#         (obj_pis_df_withress_val, obj_pis_df_withress_count), axis=1)
#     obj_pis_df_withress_concat['fund'] = fund

#     return obj_pis_df_withress_concat

# obj_by_pi_count = get_obj_by_pi_count(opd).set_index('pi')


# obj_by_pi_count_lic = get_obj_by_pi_count(opd[opd[u'Вид документа регистрации1)'] == u'Лицензии'],
#                                           fund=u'в т. ч. распред.').set_index('pi')
# obj_by_pi_count = pd.DataFrame(
#     obj_by_pi_count, index=obj_by_pi_count_lic.index)
# obj_by_pi_count_proc = obj_by_pi_count_lic.select_dtypes(exclude=['object']) / \
#     obj_by_pi_count.select_dtypes(exclude=['object']) * 100
# obj_by_pi_count_proc['fund'] = u'доля распред. %'

# obj_by_pi_count = pd.concat(
#     [obj_by_pi_count, obj_by_pi_count_lic, obj_by_pi_count_proc])
# obj_by_pi_count = obj_by_pi_count.reset_index()

# obj_pis_df_withress_concat = get_ress_df_concat(
#     opd, list(pi_cols.values()), u'в т.ч. с ресурсами')
# obj_pis_df_withress_concat_lic = get_ress_df_concat(opd[opd[u'Вид документа регистрации1)'] == u'Лицензии'],
#                                                     list(pi_cols.values()),
# u'в т. ч. распред. с ресурсами')

# obj_by_pi_count_proc = obj_by_pi_count_lic.select_dtypes(exclude=['object']) / \
#     obj_by_pi_count.select_dtypes(exclude=['object']) * 100
# # obj_pis_df_withress_concat_lic = pd.DataFrame()

# a = obj_pis_df_withress_concat
# if not obj_pis_df_withress_concat_lic.empty:
#     # a = pd.DataFrame(a, index=obj_pis_df_withress_concat_lic.index)
#     #    obj_pis_df_withress_concat_proc = obj_pis_df_withress_concat_lic.select_dtypes(exclude=['object'])/ \
#     #                                      a.select_dtypes(exclude=['object']) * 100
#     #    obj_pis_df_withress_concat_proc['fund'] = u'доля распред.'
#     #    a = pd.concat([a, obj_pis_df_withress_concat_lic, obj_pis_df_withress_concat_proc])
#     a = pd.concat([a, obj_pis_df_withress_concat_lic])

# a = a.reset_index()
# a = pd.concat([a, obj_by_pi_count])
# a = a[a['pi'].isin(list(obj_by_pi_count_lic.index))]

# a['fund'] = pd.Categorical(a['fund'], [u"всего", u"в т.ч. с ресурсами",
#                                        u"в т. ч. распред.", u"в т. ч. распред. с ресурсами", u"доля распред. %"])
# a = a.sort_values(['pi', 'fund', 'unit'])
# cs = ['P3', 'P2', 'P1', 'non_cat', 'C2']
# sort_cols = list(chain(*zip(t + ' ' + c for c in cs for t in ('val', 'cnt'))))
# sort_cols.insert(8, 'total_val')
# sort_cols.insert(9, 'total_cnt')
# sort_cols = ['pi', 'fund', 'unit'] + sort_cols
# a[sort_cols].to_csv('vovlech_kem_1603.csv', sep=';')

# vovlech = a[a['fund'] == 'доля распред. %']
# vovlech['total_cnt'] = vovlech['total_cnt'].round(0).astype(str)
# vovlech['text'] = vovlech.apply(lambda row: ': '.join(
#     ['- ' + row['pi'], row['total_cnt'] + '%;']), axis=1)
# text = vovlech['text'].to_string(index=False)

# # with open('vovlech_text.doc', 'w') as f:
# # f.write(text)

# text = text + '\n'

# groups = []
# na_vovlech = data.loc[~data['Название ПИ по ГБЗ'].isin(vovlech['pi'].unique()), [
#     'Название ПИ по ГБЗ', 'Группа ПИ в госпрограмме3)']].drop_duplicates('Название ПИ по ГБЗ')
# for group in na_vovlech.groupby('Группа ПИ в госпрограмме3)'):
#     groups.append(', '.join(group[1]['Название ПИ по ГБЗ'].tolist()))
# groups = ';\n'.join(groups)

# with open('vovlech.doc', 'w') as f:
#     f.write(text)
#     f.write(groups)
