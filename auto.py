#!/usr/local/bin/python
# -*- coding: utf-8 -*-

__author__ = "Harold F. Murcia | Sergio A. Balaguera | Cristian F. Rubio Aguiar."
__copyright__ = "Copyright 2020, Sistema de Respuesta COVID-19, Tolima"
__version__ = "1.0.4"
__status__ = "Dev"


# 1) Necessary libraries

import numpy as np
import pandas as pd
import time
import os
import sys

import warnings
warnings.filterwarnings('ignore')


# 2) Define download path

path_main = os.path.abspath("")
bash_script = """
clear
rm data/Casos_positivos_de_COVID-19_en_Colombia.csv
echo "Downloading INS DATA"
curl https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD > data/Casos_positivos_de_COVID-19_en_Colombia.csv
echo done
ls
"""


# 3) Data processing

class conditioning(object):
    def __init__(self, df):
        self.df = df
        self.df = self.df.drop(self.df.index[[0]])
        correct_name = "Archipiélago de San Andrés y Providencia"
        correct_name_2 = "Mariquita"
        print("Correct version")
        self.san_andres_adjust(correct_name)
        self.Mariquita_adjust(correct_name_2)
        self.date_adjust()
        self.adjust_format()

    def san_andres_adjust(self, correct_name):
        self.df = self.df.replace(
            "Archipiélago de San Andrés Providencia y Santa Catalina", correct_name)

    def Mariquita_adjust(self, correct_name):
        self.df = self.df.replace("San Sebastián de Mariquita", correct_name)

    def date_adjust(self):
        # Colombia
        Data_rank = self.df.groupby(['departamento']).count()
        Data_rank = Data_rank[['id_de_caso', 'edad']]
        Data_rank['id_de_caso'] = Data_rank.index
        Data_rank.columns = ['departamento', 'Total casos']
        Data_rank = Data_rank.sort_values(
            by=['Total casos'], ascending=False)
        print(Data_rank)
        Data_rank.to_csv("data/ranking_acumulado.csv",
                         index=False, encoding='utf-8-sig')

        # Tolima
        self.df = self.df.sort_values(by=['fecha_reporte_web'])
        print(self.df.shape)
        Tolima_acum = self.df
        drop_rows = Tolima_acum.loc[(Tolima_acum['departamento'] != 'TOLIMA')]
        Tolima_acum["ID_Tolima"] = ""
        Tolima_acum = Tolima_acum.drop(drop_rows.index)
        N_cases = Tolima_acum.shape[0]
        for k in range(0, N_cases):
            i = Tolima_acum.index[k]
            pos = Tolima_acum.index[Tolima_acum['id_de_caso']
                                    == Tolima_acum['id_de_caso'][i]]
            Tolima_acum['ID_Tolima'][pos] = int(k+1)
        Tolima_acum = Tolima_acum[[
            'fecha_diagnostico', 'ID_Tolima', 'ciudad_de_ubicaci_n']]
        Tolima_acum.to_csv("data/Tolima.csv", index=False,
                           encoding='utf-8-sig')

    def adjust_format(self):
        tolima_data = pd.read_csv('data/Tolima.csv')
        tolima_data = tolima_data.fillna(method="ffill")

        tolima_data['Object_ID'] = tolima_data['ID_Tolima']
        tolima_data = tolima_data.reindex(
            columns=['Object_ID', 'fecha_diagnostico', 'ID_Tolima', 'ciudad_de_ubicaci_n'])
        tolima_data.to_csv('data/Tolima.csv', index=False,
                           encoding='utf-8-sig')


if __name__ == "__main__":
    os.system("bash -c '%s'" % bash_script)
    csv_path = os.getcwd()
    csv_path = os.path.join(
        csv_path, 'data/Casos_positivos_de_COVID-19_en_Colombia.csv')
    columnas = ['fecha_reporte_web', 'id_de_caso', 'fecha_de_notificaci_n', 'cod_depto', 'departamento', 'codigo_divipola', 'ciudad_de_ubicaci_n', 'edad', 'uuu', 'sexo', 'tipo', 'ubicacion_caso', 'estado',
                'cod_pais', 'pa_s_de_procedencia', 'atenci_n', 'fis', 'fecha_de_muerte', 'fecha_diagnostico', 'fecha_recuperado', 'tipo_recuperaci_n', 'pertenencia_etnica', 'nombre_grupo_etnico']
    data = pd.read_csv(csv_path, names=columnas)
    result = conditioning(data)
