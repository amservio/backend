# ------------
# - IMPORTS
# ------------

import streamlit as st
import pandas as pd
import geopandas as gpd
import numpy as np
import json
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

# ------------
# - MAIN
# ------------

ano_selecionado = st.selectbox('Selecionar Ano', anos_disponiveis)

url = f"http://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados/SPDadosCriminais_{ano_selecionado}.xlsx"

df = pd.read_parquet(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.parquet"))
if 'DATA_COMUNICACAO_BO' in df and df['DATA_COMUNICACAO_BO'].dtype != 'string':
    df['DATA_COMUNICACAO_BO'] = df['DATA_COMUNICACAO_BO'].dt.strftime('%Y-%m-%d %H:%M:%S')
if 'DATA_COMUNICACAO' in df.columns and df['DATA_COMUNICACAO'].dtype != 'string':
    df['DATA_COMUNICACAO'] = df['DATA_COMUNICACAO'].dt.strftime('%Y-%m-%d %H:%M:%S')
if 'DATA_OCORRENCIA_BO' in df.columns and df['DATA_OCORRENCIA_BO'].dtype != 'string':
    df['DATA_OCORRENCIA_BO'] = df['DATA_OCORRENCIA_BO'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
df = df.fillna(np.nan).replace([np.nan], [None])
st.dataframe(df.head(20))


if st.button("Create GeoJSON"):
    geojson = {
        "type": "FeatureCollection",
        "features": []
    }

    properties = [col for col in df.columns if col not in ['LONGITUDE', 'LATITUDE']]

    for idx, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['LONGITUDE'], row['LATITUDE']]
            },
            "properties":{
                col: row[col] for col in properties
            }
        }

        geojson['features'].append(feature)

    with open(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.geojson"), 'w') as f:
        json.dump(geojson, f)

if st.button('Create GeoParquet'):
    gdf = gpd.read_file(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.geojson"))
    gdf = gdf.to_wkt()
    gdf.to_parquet(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.geoparquet"))
    data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.geojson").unlink()