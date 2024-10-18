# ------------
# - IMPORTS
# ------------

import streamlit as st
import pandas as pd
import geopandas as gpd
import numpy as np
import json
import geojson
import requests
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import warnings
import pathlib

import sys
if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO

# ------------
# - SETUP
# ------------

warnings.filterwarnings('ignore')
st.set_page_config(
    page_title='Backend Mapa da Criminalidade SP',
    layout="wide"
)

tmp_folder = pathlib.Path(__file__) \
                .parent.parent \
                .joinpath('tmp')
data_folder = pathlib.Path(__file__) \
                .parent.parent \
                .joinpath('data')

anos_disponiveis = [2022, 2023, 2024]

# ------------
# - FUNCTIONS
# ------------

def get_crime_data_parquet(year, nrows=None):
    url = f"http://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_{year}.xlsx"
    r = requests.get(url)
    with open(tmp_folder.joinpath('tmp.xlsx'), 'wb') as f:
        f.write(r.content)
    tabs = pd.ExcelFile(tmp_folder.joinpath('tmp.xlsx')).sheet_names
    df = pd.DataFrame()
    for tab in tabs:
        if nrows:
            sheet = pd.read_excel(tmp_folder.joinpath('tmp.xlsx'), sheet_name=tab, nrows=nrows)
        else:
            sheet = pd.read_excel(tmp_folder.joinpath('tmp.xlsx'), sheet_name=tab)
        sheet['LATITUDE'] = sheet['LATITUDE'].astype('string').str.replace(',.', ',')
        sheet['LONGITUDE'] = sheet['LONGITUDE'].astype('string').str.replace(',.', ',')
        sheet['LATITUDE'] = sheet['LATITUDE'].str.replace(',', '.').astype('float')
        sheet['LONGITUDE'] = sheet['LONGITUDE'].str.replace(',', '.').astype('float')
        sheet['DATA_OCORRENCIA_BO'] = sheet['DATA_OCORRENCIA_BO'].astype('string')
        sheet['HORA_OCORRENCIA_BO'] = sheet['HORA_OCORRENCIA_BO'].astype('string')
        sheet['NUM_BO'] = sheet['NUM_BO'].astype('string')
        sheet['BAIRRO'] = sheet['BAIRRO'].astype('string')
        sheet['LOGRADOURO'] = sheet['LOGRADOURO'].astype('string')
        sheet['NUMERO_LOGRADOURO'] = sheet['NUMERO_LOGRADOURO'].astype('string')
        df = pd.concat([df, sheet])
    df.to_parquet(data_folder.joinpath(f"SPDadosCriminais_{year}.parquet"))
    tmp_folder.joinpath('tmp.xlsx').unlink()

def get_crime_data_geoparquet(year, properties):
    df = pd.read_parquet(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.parquet"))
    df['LONGITUDE'] = df['LONGITUDE'].fillna(0)
    df['LATITUDE'] = df['LATITUDE'].fillna(0)
    df['BAIRRO'] = df['BAIRRO'].fillna('Não informado')
    df['DESCR_TIPOLOCAL'] = df['DESCR_TIPOLOCAL'].fillna('Não informado')
    features = []
    for idx, row in df.iterrows():
        if row['LONGITUDE'] > -180 and row['LONGITUDE'] < 180 and row['LATITUDE'] > -90 and row['LATITUDE'] < 90:
            point = geojson.Point((row['LONGITUDE'], row['LATITUDE']))
            features.append(geojson.Feature(geometry=point, properties={prop: row[prop] for prop in properties}))
    feature_collection = geojson.FeatureCollection(features=features)
    with open(tmp_folder.joinpath('tmp.json'), 'w') as f:
        geojson.dump(feature_collection, f)
    gdf = gpd.read_file(tmp_folder.joinpath('tmp.json'))
    gdf.to_parquet(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.geoparquet"), geometry_encoding='geoarrow')
    tmp_folder.joinpath('tmp.json').unlink()
    
# ------------
# - MAIN
# ------------

ano_selecionado = st.selectbox('Selecionar Ano', anos_disponiveis)

col1, col2, col3, _, _, _ = st.columns(6)
if col1.button('Create Parquet File'):
    get_crime_data_parquet(ano_selecionado)

if col2.button('Create Geoparquet File'):
    get_crime_data_geoparquet(ano_selecionado, properties=['MES_ESTATISTICA', 'ANO_ESTATISTICA', 'NATUREZA_APURADA', 'BAIRRO', 'DESCR_TIPOLOCAL'])

if col3.button('Join Geoparquet Files'):
    files = data_folder.glob('*.geoparquet')
    df_geoparquet = pd.DataFrame()
    for file in files:
        df = pd.read_parquet(file)
        df_geoparquet = pd.concat([df_geoparquet, df])
    df_geoparquet.to_parquet('./data/SPDadosCriminais.geoparquet')

col1, col2 = st.columns(2)

if data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.parquet").exists():
    df_parquet = pd.read_parquet(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.parquet"))
    col1.dataframe(df_parquet.head(20), use_container_width=True, hide_index=True)
    col1.dataframe(df_parquet.query('LATITUDE != 0 or LONGITUDE != 0').groupby(['NATUREZA_APURADA']).agg({
        'NUM_BO': np.size
    }), use_container_width=True)
    col1.dataframe(df_parquet.query('LATITUDE == 0 and LONGITUDE == 0').groupby(['NATUREZA_APURADA']).agg({
        'NUM_BO': np.size
    }), use_container_width=True)

if data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.geoparquet").exists():
    df_geoparquet = pd.read_parquet(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.geoparquet"))
    col2.dataframe(df_geoparquet.head(20), use_container_width=True, hide_index=True)
