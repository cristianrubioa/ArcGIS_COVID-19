#!/usr/local/bin/python
# -*- coding: utf-8 -*-

__author__ = "Harold F. Murcia | Sergio A. Balaguera | Cristian F. Rubio Aguiar."
__copyright__ = "Copyright 2020, Sistema de Respuesta COVID-19, Tolima"
__version__ = "0.1.0"
__status__ = "Dev"


# 1) Necessary libraries

import numpy as np
import pandas as pd
import time, os, sys

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
        self.df = self.df.replace("Archipiélago de San Andrés Providencia y Santa Catalina", correct_name)

    def Mariquita_adjust(self, correct_name):
        self.df = self.df.replace("San Sebastián de Mariquita", correct_name)

    def date_adjust(self):
        self.df.fecha_de_notificaci_n = self.df.fecha_de_notificaci_n.str.replace("T00:00:00.000","")
        self.df.fecha_diagnostico = self.df.fecha_diagnostico.str.replace("T00:00:00.000","")
        self.df.fecha_diagnostico = self.df.fecha_diagnostico.str.replace("-   -","")
        self.df.fecha_diagnostico = self.df.fecha_diagnostico.str.replace("SIN DATO","")
        self.df.fecha_recuperado = self.df.fecha_recuperado.str.replace("T00:00:00.000","")
        self.df.fecha_recuperado = self.df.fecha_recuperado.str.replace("-   -","")
        self.df.fecha_reporte_web = self.df.fecha_reporte_web.str.replace("T00:00:00.000","")
        self.df.fecha_reporte_web = self.df.fecha_reporte_web.str.replace("-   -","")
        self.df.fecha_de_muerte = self.df.fecha_de_muerte.str.replace("T00:00:00.000","")
        self.df.fecha_de_muerte = self.df.fecha_de_muerte.str.replace("-   -","")
        self.df.fis = self.df.fis.str.replace("T00:00:00.000","")
        self.df.fis = self.df.fis.str.replace("Asintomático","")
        self.df.fecha_de_notificaci_n = pd.to_datetime(self.df['fecha_de_notificaci_n'])
        self.df.fecha_de_notificaci_n = self.df.fecha_de_notificaci_n.dt.strftime('%m/%d/%Y')
        self.df.fecha_diagnostico = pd.to_datetime(self.df['fecha_diagnostico'])
        self.df.fecha_diagnostico = self.df.fecha_diagnostico.dt.strftime('%m/%d/%Y')
        print(self.df.fecha_reporte_web)
        self.df.fecha_reporte_web = pd.to_datetime(self.df['fecha_reporte_web'])
        self.df.fecha_reporte_web = self.df.fecha_reporte_web.dt.strftime('%m/%d/%Y')
        self.df.to_csv("data/Casos_positivos_de_COVID-19_en_Colombia_filtered.csv", index=False, encoding='utf-8-sig')

        # Colombia
        Data_rank = self.df.groupby(['departamento']).count()
        Data_rank = Data_rank[['id_de_caso','edad']]
        Data_rank['id_de_caso'] = Data_rank.index
        Data_rank.columns = ['departamento','Total casos']
        Data_rank=Data_rank.sort_values(by=['Total casos'], ascending=False)
        print(Data_rank)
        Data_rank.to_csv("data/ranking_acumulado.csv", index=False, encoding='utf-8-sig')

        # Tolima
        self.df = self.df.sort_values(by=['fecha_reporte_web'])
        print(self.df.shape)
        Tolima_acum = self.df
        drop_rows = Tolima_acum.loc[(Tolima_acum['departamento']!='Tolima')]
        Tolima_acum["ID_Tolima"] = ""
        Tolima_acum = Tolima_acum.drop(drop_rows.index)
        N_cases = Tolima_acum.shape[0]
        for k in range(0,N_cases):
            i = Tolima_acum.index[k]
            pos = Tolima_acum.index[Tolima_acum['id_de_caso'] == Tolima_acum['id_de_caso'][i] ]
            Tolima_acum['ID_Tolima'][pos] = int(k+1)
        Tolima_acum = Tolima_acum[['fecha_diagnostico','ID_Tolima','ciudad_de_ubicaci_n']]
        Tolima_acum.to_csv("data/Tolima.csv", index=False, encoding='utf-8-sig')

    def adjust_format(self):
        tolima_data = pd.read_csv('data/Tolima.csv')            
        tolima_data = tolima_data.fillna(method="ffill")

        tolima_data['fecha_diagnostico']=pd.to_datetime(tolima_data['fecha_diagnostico'].astype(str), format='%m/%d/%Y')
        tolima_data['fecha_diagnostico'] = tolima_data['fecha_diagnostico'].dt.strftime('%d-%m-%Y')

        tolima_data['Object_ID'] = tolima_data['ID_Tolima']
        tolima_data = tolima_data.reindex(columns=['Object_ID','fecha_diagnostico', 'ID_Tolima', 'ciudad_de_ubicaci_n'])
        tolima_data.to_csv('data/Tolima.csv', index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    #os.system("bash -c '%s'" % bash_script)
    csv_path = os.getcwd()
    csv_path = os.path.join(csv_path, 'data/Casos_positivos_de_COVID-19_en_Colombia.csv')
    columnas = ['id_de_caso','fecha_de_notificaci_n','codigo_divipola','ciudad_de_ubicaci_n','departamento','atenci_n','edad','sexo','tipo','estado','pa_s_de_procedencia','fis','fecha_de_muerte','fecha_diagnostico','fecha_recuperado','fecha_reporte_web','tipo_recuperaci_n','cod_depto','cod_pais','Pertenencia_etnica','Nombre_grupo_etnico', 'nuevo']
    data = pd.read_csv(csv_path, names=columnas)
    result = conditioning(data)