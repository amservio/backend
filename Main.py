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

# ------------
# - FUNCTIONS
# ------------

# ------------
# - MAIN
# ------------
