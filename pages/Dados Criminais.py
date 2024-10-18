# ------------
# - IMPORTS
# ------------

import streamlit as st
import pandas as pd
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
st.write(url)

if st.button('Buscar'):
    r = requests.get(url)
    with open(tmp_folder.joinpath('tmp.xlsx'), 'wb') as f:
        f.write(r.content)
    tabs = pd.ExcelFile(tmp_folder.joinpath('tmp.xlsx')).sheet_names
    df = pd.DataFrame()
    for tab in tabs:
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
    st.dataframe(df.head(50))
    df.to_parquet(data_folder.joinpath(f"SPDadosCriminais_{ano_selecionado}.parquet"))
    tmp_folder.joinpath('tmp.xlsx').unlink()