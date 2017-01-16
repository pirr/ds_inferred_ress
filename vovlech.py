# -*- coding: utf-8 -*-


import pandas as pd
from itertools import chain

pd.options.display.max_columns = 50

pi_cols = {u'Название ПИ по ГБЗ': 'pi', u'Ед. измерения ПИ': 'unit'}
cat_cols = {u'P3': 'P3', u'P2': 'P2', u'P1': 'P1', u'Без категор.': 'non_cat', u'С2': 'C2'}
all_cols = pi_cols.copy()
all_cols.update(cat_cols)

#file_path = u'd://work//!Прогнозные ресурсы//АК, РА//reestr_ak-ra_gkm_05082016.xls'
file_path = u'd://Smaga//!EXCHENGE//Марина//a_reestr_io_200916.xls'
data = pd.read_excel(file_path, header=1)
data = data[data[u'Актуальность строки']=='А'][1:]
data[u'Название ПИ по ГБЗ'] = data[u'Название ПИ по ГБЗ'].str.capitalize()
#data = data[data[u'Субъект РФ']==u'Республика Алтай']
obj_pis_df = data[list(all_cols.keys()) + [u'№ объекта', u'Вид документа регистрации1)']]

opd = obj_pis_df.rename(columns=all_cols)

def get_obj_pis_group(df, pi_cols):
    if type(pi_cols) is not list:
        pi_cols = [pi_cols]
    obj_pis_df_withress = df.groupby(pi_cols + [u'№ объекта',]).max().reset_index()
    
    if obj_pis_df_withress.empty:
        return False
    
    obj_pis_df_withress = obj_pis_df_withress.drop(u'№ объекта', axis=1)
    obj_pis_df_withress = obj_pis_df_withress.drop(u'Вид документа регистрации1)', axis=1)
    obj_pis_df_withress = obj_pis_df_withress.groupby(pi_cols)
    return obj_pis_df_withress

def get_total_val_by_cols(df, val_name='val'):
    total_val_name = 'total_' + val_name
    val_cols = {c: val_name + ' ' + c for c in cat_cols.values()}
    inf_cols_val = [c for c in val_cols.values() if 'C2' not in c]
    
    if val_name == 'val':
        obj_pis_df_withress_val = df.sum()
    elif val_name == 'cnt':
        obj_pis_df_withress_val = df.count()
        
    obj_pis_df_withress_val = obj_pis_df_withress_val.rename(columns=val_cols)
    obj_pis_df_withress_val[total_val_name] = obj_pis_df_withress_val[inf_cols_val].sum(axis=1)
    return obj_pis_df_withress_val

def get_obj_by_pi_count(df, fund=u'всего'):
    obj_by_pi = get_obj_pis_group(df, 'pi').size().to_frame().rename(columns={0:'total_cnt'})
    obj_by_pi['fund'] = fund
    return obj_by_pi.reset_index()

def get_ress_df_concat(df, pi_cols, fund=u'фонд'):
    obj_pis_df_withress = get_obj_pis_group(df,pi_cols)
    
    if not obj_pis_df_withress:
        print(u'Empty dataframe in {}!'.format(fund))
        return pd.DataFrame(columns=all_cols)
    
    obj_pis_df_withress_val = get_total_val_by_cols(obj_pis_df_withress, 'val')
    obj_pis_df_withress_count = get_total_val_by_cols(obj_pis_df_withress, 'cnt')
    obj_pis_df_withress_concat = pd.concat((obj_pis_df_withress_val, obj_pis_df_withress_count), axis=1)
    obj_pis_df_withress_concat['fund'] = fund
    
    return obj_pis_df_withress_concat
    
obj_by_pi_count = get_obj_by_pi_count(opd).set_index('pi')
obj_by_pi_count_lic = get_obj_by_pi_count(opd[opd[u'Вид документа регистрации1)']==u'лицензии'],
                                          fund='в т. ч. распред.').set_index('pi')
obj_by_pi_count = pd.DataFrame(obj_by_pi_count, index=obj_by_pi_count_lic.index)
obj_by_pi_count_proc = obj_by_pi_count_lic.select_dtypes(exclude=['object'])/ \
                                      obj_by_pi_count.select_dtypes(exclude=['object']) * 100
obj_by_pi_count_proc['fund'] = u'доля распред. %'

obj_by_pi_count = pd.concat([obj_by_pi_count, obj_by_pi_count_lic, obj_by_pi_count_proc])
obj_by_pi_count = obj_by_pi_count.reset_index()

obj_pis_df_withress_concat = get_ress_df_concat(opd, list(pi_cols.values()), u'в т.ч. с ресурсами')
obj_pis_df_withress_concat_lic = get_ress_df_concat(opd[opd[u'Вид документа регистрации1)']==u'лицензии'], 
                                                    list(pi_cols.values()), 
                                                    u'в т. ч. распред. с ресурсами')

a = obj_pis_df_withress_concat
if not obj_pis_df_withress_concat_lic.empty:
    a = pd.DataFrame(a, index=obj_pis_df_withress_concat_lic.index)
#    obj_pis_df_withress_concat_proc = obj_pis_df_withress_concat_lic.select_dtypes(exclude=['object'])/ \
#                                      a.select_dtypes(exclude=['object']) * 100
#    obj_pis_df_withress_concat_proc['fund'] = u'доля распред.'
#    a = pd.concat([a, obj_pis_df_withress_concat_lic, obj_pis_df_withress_concat_proc])
    a = pd.concat([a, obj_pis_df_withress_concat_lic])

a = a.reset_index()
a = pd.concat([a, obj_by_pi_count])
a = a[a['pi'].isin(list(obj_by_pi_count_lic.index))]

a['fund'] = pd.Categorical(a['fund'], [u"всего", u"в т.ч. с ресурсами", u"в т. ч. распред.",  u"в т. ч. распред. с ресурсами", u"доля распред. %"])
a = a.sort_values(['pi', 'fund', 'unit'])
cs = ['P3', 'P2', 'P1', 'non_cat', 'C2']
sort_cols = list(chain(*zip(t+' '+c for c in cs for t in ('val', 'cnt'))))
sort_cols.insert(8, 'total_val')
sort_cols.insert(9, 'total_cnt')
sort_cols = ['pi', 'fund', 'unit'] + sort_cols
a[sort_cols].to_csv('vovlech_test_3110.csv', sep=';')