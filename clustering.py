# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 10:46:22 2016

@author: Smaga
"""

import pandas as pd
import numpy as np

import re
import string
import pymorphy2
from fuzzywuzzy import fuzz
from multiprocessing import Pool
from itertools import product

from sklearn.feature_extraction.text import CountVectorizer, HashingVectorizer
from sklearn.cluster import MeanShift, AffinityPropagation, DBSCAN, Birch


exclude = re.compile('[%s]' % string.punctuation)
pi_df = pd.read_csv('D://Smaga//bitbucket//obj_creator//dict//pi2.csv',
                    delimiter=';', encoding='cp1251')
name_patterns = pd.read_csv(
    'D://Smaga//bitbucket//obj_creator//dict//pattern_for_replace.csv', delimiter=';', encoding='cp1251')
morph = pymorphy2.MorphAnalyzer()


def get_analysis_name(name):
    analysis_name = exclude.sub(r' ', name)

    for p in name_patterns:
        if p[2] == 'replace':
            analysis_name = analysis_name.replace(p[0], p[1])
        if p[2] == 'regex_sub':
            analysis_name = re.sub(
                r'%s' % p[0], r'%s' % p[1], analysis_name)

    return analysis_name


def f_tokenizer(s):

    if type(s) == unicode:
        t = s.split(' ')
    else:
        t = s
    f = []
    for j in t:
        wrd = morph.parse(j)[0]
#        m = morph.parse(j.replace('.',''))
        if wrd.tag.POS not in ('NUMR', 'PREP', 'CONJ', 'PRCL', 'INTJ') and len(j) > 1:
            f.append(wrd.normal_form)
    return ' '.join(f)


def names_comp(names):
    return fuzz.partial_ratio(names[0], names[1])

if __name__ == '__main__':
    file_path = u'd://work//!Прогнозные ресурсы//АК, РА//reestr_ak-ra_gkm_05082016.xls'
    # file_path = u'd://Smaga//!EXCHENGE//Марина//a_reestr_io_200916.xls'
    data = pd.read_excel(file_path, header=1)
    data = data[1:]
    data[u'Название ПИ по ГБЗ'] = data[u'Название ПИ по ГБЗ'].str.capitalize()
    data = data[data[u'Субъект РФ'] == u'Республика Алтай']

    X = data[[u'Название объекта', u'Координата центра Y',
              u'Координата центра X', u'ПИ (перечень для объекта)', u'№ объекта']]
    X.columns = [u'name', u'lat', u'lon', u'pi', u'obj_num']
    X = X.dropna()
    X[u'name'] = X[u'name'].str.lower()
    X[u'name'] = X[u'name'].apply(lambda name: get_analysis_name(name))

    X = pd.merge(X, pi_df, right_index=True, on=[u'pi'])
    pis = np.unique(X[u'group_pi'])
    pis_numeriq = {pis[i]: i for i in range(len(pis))}
    X[u'group_pi'] = X[u'group_pi'].apply(lambda pi: pis_numeriq[pi])
    X = X[:100]
    p = Pool(5)
    names = p.map(f_tokenizer, X['name'])
    names_ratio = np.array(map(names_comp, product(
        X['name'], repeat=2))).reshape([len(X), len(X)])
    names_ratio_tok = np.array(map(names_comp, product(
        names, repeat=2))).reshape([len(X), len(X)])

    i = 0
    while i < len(X):
        names_ratio[i, :i + 1] = 0
        names_ratio_tok[i, :i + 1] = 0
        i += 1

    names_ratio = names_ratio[:-1]
    names_ratio_tok = names_ratio_tok[:-1]
    names_couples = np.argwhere(names_ratio > 80)
    names_couples_tok = np.argwhere(names_ratio_tok > 80)

#name_combos = list(combinations(X['name'], 2))
# print(len(name_combos))
#vect = CountVectorizer(tokenizer=f_tokenizer, max_df=30)
#trn = vect.fit_transform(X[u'name'])
#ws = vect.get_feature_names()
#
#ap = AffinityPropagation(damping=0.9, max_iter=200, convergence_iter=10)
#X['cluster_by_name_ap_d9'] = ap.fit_predict(trn.toarray())
