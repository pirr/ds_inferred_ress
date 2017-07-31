import os
import re
from collections import OrderedDict

import numpy as np
import pandas as pd

# from _help_fun import flash_mess, message_former_from


REGISTRY_COLUMNS = OrderedDict([(u'№ строки', 'N'),
                                (u'Актуальность строки', 'actual'),
                                (u'№ изменений', 'N_change'),
                                (u'Операция внесения (добавление, изменение, удаление)',
                                 'change_type'),
                                (u'№ объекта', 'N_obj'),
                                (u'Признак комплексного', 'complex'),
                                (u'Вид документа регистрации1)', 'doc_type'),
                                (u'Наличие ГКМ паспорта в группе', 'obj_with_gkm'),
                                (u'Орган регистрации (ТФИ, РГФ, ВСЕГЕИ, ЦНИГРИ, Роснедра, Минприроды, ГСЭ)',
                                 'organ_regs'),
                                (u'Номер документа', 'doc_num'),
                                (u'Дата регистрации', 'doc_date'),
                                (u'Год регистрации (для сортировки)', 'doc_date_num'),
                                (u'№ объекта в документе регистрации',
                                 'obj_num_in_doc'),
                                (u'Федеральный округ', 'fed_distr'),
                                (u'Субъект РФ', 'subj_distr'),
                                (u'Административный район', 'adm_distr'),
                                (u'Лист м-ба 1000', '1000_map'),
                                (u'Лист м-ба 200 (араб.)', '200_map'),
                                (u'Вид объекта2)', 'geol_type_obj'),
                                (u'Название объекта', 'name_obj'),
                                (u'Фонд недр (Р-распред., НР-нераспред.)', 'fund'),
                                (u'Вид пользования недрами (ГИН/Р+Д/ГИН+Р+Д)', 'use_type'),
                                (u'Группа ПИ в госпрограмме3)', 'gover_type_pi'),
                                (u'ПИ (перечень для объекта)', 'pi'),
                                (u'Название нормализ.', 'norm_pi'),
                                (u'Название ПИ по ГБЗ', 'gbz_pi'),
                                (u'Группа ПИ ИС недра', 'isnedra_pi'),
                                (u'Ед. измерения ПИ', 'unit_pi'),
                                (u'P3', 'P3_cat'),
                                (u'P2', 'P2_cat'),
                                (u'P1', 'P1_cat'),
                                (u'С2', 'C2_res'),
                                (u'Без категор.', 'none_cat'),
                                (u'Запасы ABC1', 'ABC_res'),
                                (u'Признак наличия ресурсных оценок', 'res_exist'),
                                (u'Наличие прогнозных ресурсов', 'cat_avaibil'),
                                (u'Признак наличия запасов', 'res_avaibil'),
                                (u'Вид документа апробации (протокол, отчет)',
                                 'probe_doc_type'),
                                (u'Номер', 'probe_doc_num'),
                                (u'Дата', 'probe_doc_date'),
                                (u'Орган апробации', 'probe_doc_organ'),
                                (u'№ в таблице координат для полигонов',
                                 'N_poly_table'),
                                (u'Территория органа апробации', 'probe_organ_subj'),
                                (u'Вид координат (Т-точка, П-полигон)', 'coord_type'),
                                (u'Площадь, км2', 'area'),
                                (u'Координата центра X', 'lon'),
                                (u'Координата центра Y', 'lat'),
                                (u'Источник координат4)', 'coord_source'),
                                (u'Входимость в лицензионыый участок', 'license_area'),
                                (u'Достоверность координат', 'coord_reliability'),
                                (u'Координаты треб. проверки', 'coord_for_check'),
                                (u'Данные о районе (для определения координат)',
                                 'territory_descript'),
                                (u'Другие документы об объекте (вид документа, №, год, стадия ГРР, авторы, организация)',
                                 'other_source'),
                                (u'Рекомендуемые работы (оценка ПР, апробация ПР, в фонд заявок, поиски, оценка и др.)',
                                 'recommendations')])

INVERT_REGISTRY_COLUMNS = OrderedDict(
    [(v, k) for k, v in REGISTRY_COLUMNS.items()])

actual_cols = ('_id', '_rev', 'id_reg', 'filename')

name_patterns = pd.read_csv(
    'D://Smaga//bitbucket//obj_creator//dict//pattern_for_replace.csv', delimiter=';', encoding='cp1251')


class RegistryExc(Exception):
    pass


class ShpRegistryExc(Exception):
    pass


class RegistryFormatter:
    u'''
        верификация и форматирование реестра для импорта в БД
    '''

    def __init__(self, registry_df, registry_cols_dict):
        self.registry = registry_df
        self.cols = registry_cols_dict
        self.errors = dict()

    # сбор ошибок верификации реестра
    def _append_errors(self, err_name, err_str):
        if err_name in self.errors:
            self.errors[err_name].extend(err_str)
        else:
            self.errors[err_name] = [err_str]

    # проверка наличия ошибок
    def check_errors(self):
        if self.errors:
            mess = message_former_from(self.errors)
            flash_mess(mess)
            raise RegistryExc(mess)

    # удаление переносов и других непробельных символов в названии колонок
    # реестра
    def columns_strip(self):
        pattern = re.compile(r'\s+')
        self.registry.columns = [pattern.sub(
            ' ', c) for c in self.registry.columns]

    # проверка наличия колонок, если отсутсвуют то записать их в
    # соотвествующую ошибку
    def check_columns(self):
        none_cols = [c for c in self.cols.keys()
                     if c not in self.registry.columns]
        if none_cols:
            self._append_errors(
                u'В реестре отсутствуют колонки', ', '.join(none_cols))

    # обновление названий колонок для БД
    def update_column_names_for_db(self):
        self.registry.columns = [self.cols[c] for c in self.registry.columns]

    # округление чисел с плавающей точкой
    def fix_float(self):
        for col in self.registry.columns:
            if self.registry[col].dtype == np.float64:
                self.registry[col] = np.round(self.registry[col], 6)

    # ошибки координат
    @staticmethod
    def check_coord(coord):

        coord_list = []
        for c in coord:
            _, str_coord_decim = str(c).split('.')
            len_str_coord_decim = len(str_coord_decim)

            if len_str_coord_decim < 3:
                return u'err'

            dupl_count = 0
            for i, d in enumerate(str_coord_decim[2:]):
                if str_coord_decim[i - 1] == str_coord_decim[i]:
                    dupl_count += 1
                else:
                    dupl_count -= 1
            if len_str_coord_decim - dupl_count >= len_str_coord_decim / 2:
                return u'err'

            coord_list.append(c)

        return coord_list

    @staticmethod
    def get_analysis_name(name):
        analysis_name = exclude.sub(r' ', str(name))

        for p in name_patterns:
            if p[2] == 'replace':
                analysis_name = analysis_name.replace(p[0], p[1])
            if p[2] == 'regex_sub':
                analysis_name = re.sub(
                    r'%s' % p[0], r'%s' % p[1], analysis_name)

        return analysis_name

    def prepare_coord(self):
        self.registry[u'coord'] = self.registry.apply(
            lambda row: self.check_coord(row['lon'], row['lat']))
        return u'coords with err: {}'.format(self.registry.loc[self.registry['coord'] == 'err', 'N'])

        # self.registry['coord'] = self.registry.loc[
        #     ~pd.isnull(self.registry['lon']), ['lon', 'lat']]
        # gkm_coords['coord'] = gkm_coords.apply(lambda coord: )

    def format(self):
        self.columns_strip()
        self.check_columns()
        self.check_errors()
#         self.fix_float()
        self.update_column_names_for_db()
        self.registry.fillna('', inplace=True)


class ShpRegistry:

    driver = ogr.GetDriverByName('ESRI Shapefile')
    srs = osr.SpatialReference()

    def __init__(self, shp_file_paths=None, shp_dir=None):
        self.shp_file_paths = shp_file_paths
        self.shp_dir = shp_dir

        if not any([self.shp_file_paths, self.shp_dir]):
            raise ShpRegistryExc('Need shp_file_paths, shp_dir')

        if self.shp_file_paths is None:
            self.shp_file_paths = self.__get_shp_file_paths()

        else:
            if type(self.shp_file_paths) != list:
                self.shp_file_paths = [self.shp_file_paths]

        if self.shp_dir is None:
            self.shp_dir = []

        self.shp_file_paths = list(set(self.shp_file_paths))

        self.df = self.__concat_df_shp()

    @staticmethod
    def __get_dataframe_from_shp(self, shp_file_path):
        shp_file = self.driver.Open(shp_file_path, 0)
        dbf_filename = shp_file_path[:-4] + '.dbf'

        try:
            dbf_file = Dbf5(dbf_filename, codec='utf-8')
            df = dbf_file.to_dataframe()
        except UnicodeDecodeError:
            dbf_file = Dbf5(dbf_filename, codec='cp1251')
            df = dbf_file.to_dataframe()
        except Exception as e:
            raise ShpRegistryExc('Problem with data in file:', dbf_filename, e)

        try:
            daLayer = shp_file.GetLayer(0)
            wkt_geom = [feature.geometry().ExportToWkt()
                        for feature in daLayer]
            spatialRef = daLayer.GetSpatialRef()
        except AttributeError as e:
            raise ShpRegistryExc(
                'Problem with geometry in file:', shp_file_path, e)

        df['_wkt_'] = wkt_geom
        df['_geom_type_'] = df['_wkt_'].str.findall(r'[A-z]+')
        df['_geom_type_'] = df['_geom_type_'].apply(lambda x: x[0])
        df['_prj_'] = spatialRef.ExportToWkt()

        return df

    def __get_shp_file_paths(self):
        return [os.path.join(self.shp_dir, sf) for sf in os.listdir(self.shp_dir) if sf.split('.')[-1] == 'shp']

    @staticmethod
    def __transform_geom_prj(geom, transform):
        geom.Transform(transform)
        return geom

    def __transform_df_prj(self, target_prj):
        for prj_type in self.df['_prj_'].unique():
            source_prj = osr.SpatialReference()
            source_prj.ImportFromWkt(prj_type)
            transform = osr.CoordinateTransformation(source_prj, target_prj)
            self.df.loc[self.df['_prj_'] == prj_type, '__geometry'] = self.df.loc[self.df[
                '_prj_'] == prj_type, '_wkt_'].apply(lambda wkt: ogr.CreateGeometryFromWkt(wkt))
            self.df.loc[self.df['_prj_'] == prj_type, '__geometry'] = self.df.loc[self.df[
                '_prj_'] == prj_type, '__geometry'].apply(lambda geom: __transform_geom_prj(geom, transform))
            self.df.loc[self.df['_prj_'] == prj_type, '_wkt_'] = self.df.loc[self.df[
                '_prj_'] == prj_type, '__geometry'].apply(lambda geom: geom.ExportToWkt())
        del self.df['__geometry']

    def __concat_df_shp(self):
        conc_df = pd.DataFrame()
        for sfp in self.shp_file_paths:
            try:
                df_shp = self.__get_dataframe_from_shp(self, sfp)
            except ShpRegistryExc as e:
                print(e)
                continue
            if conc_df.empty:
                conc_df = df_shp
            else:
                conc_df = conc_df.append(df_shp, ignore_index=True)

        return conc_df

    def transform_to_only_one_projection(self, target_prj=None):
        if len(self.df['_prj_'].unique()) > 1:
            if target_prj is None:
                srs = osr.SpatialReference()
                target_prj = srs.ImportFromEPSG(4326)
            self.__transform_df_prj(target_prj)

    def get_point_xy(self):
        self.df.loc[df['_geom_type_'] == 'POINT', 'xy'] = self.df.loc[
            df['_geom_type_'] == 'POINT', '_wkt_'].apply(lambda wkt: (wkt.GetX(), wkt.GetY()))
