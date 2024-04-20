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

stock_monitor = OrderedDict()

@app.callback(
    Output('stock-track', 'figure'),
    [Input('interval-comp', 'interval')]
)
def live_graph_update(interval):
    # Read published messages from Redis list
    fetched_data = redis_client.lrange('stock_monitor', 0, -1)
    all_tickers_dates_n_prices = {'WMT': {'date': ['2024-04-18', '2024-04-19', '2024-04-20'], 
                                          'price': [145.23, 146.05, 144.78]}, 
                                  'NVDA': {'date': ['2024-04-01', '2024-04-30','2024-04-11'], 
                                           'price': [330.80, 328.45, 329.60]}}
    for msg in fetched_data:
        stock_monitor[msg['symbol']] = (msg['date'], round(msg['price'], 2))  
        current_ticker = msg['symbol']
        current_date = stock_monitor[current_ticker][0]
        current_price = stock_monitor[current_ticker][1]
        
        all_tickers_dates_n_prices[current_ticker]['date'].append(current_date)
        all_tickers_dates_n_prices[current_ticker]['price'].append(current_price)
        
    fig = px.line()
    
    for ticker, data in all_tickers_dates_n_prices.items():
        fig.add_trace(
            x=data['date'], y=data['price'], mode='lines', name=ticker, 
            hoverinfo='y+name', hovertemplate='<b>%{y}</b><br>%{name}'
        )
    
    # Adjusting y-axis scale to accommodate large variance in prices
    fig.update_yaxes(type="log")
    
    # Other parameters
    fig.update_layout(
        title="Stock Prices Over Time",
        title_x=0.5,  # Title position
        showlegend=True,  # Show legend
        hovermode="x"  # Show hover information only on x-axis
    )
    
    return fig
``


if __name__ == "__main__":
    app.run(debug=True)
