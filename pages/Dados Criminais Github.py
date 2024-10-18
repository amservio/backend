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

url = f"https://raw.githubusercontent.com/amservio/mapacriminalidadesp-backend/main/data/SPDadosCriminais_{ano_selecionado}.parquet"

df = pd.read_parquet(url)

st.dataframe(df.head(500), hide_index=True, use_container_width=True)