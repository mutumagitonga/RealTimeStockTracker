from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import redis
from collections import OrderedDict

# Start redis
redis_client = redis.Redis(host='localhost', port=6379)

# Configure and create dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div(
    [
        html.H1("Latest Stock Prices", style={"text-align": "center"}),
        dbc.Graph(id="stock-track", figure={}),
        dcc.Interval(id='interval-comp', interval=3000, n_intervals=0)
    ]
)
