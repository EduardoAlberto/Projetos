# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.


from dash import Dash, html, dcc, dash_table
import plotly.express as px
from dbconfig import *

app = Dash(__name__)

df = selectVideogame()

fig = px.bar(df, x="genres", y="publishers", color="console", barmode="group")

app.layout = html.Div(children=[
    html.H1(children='Video Game Dash'),
    

    html.Div(children=''' Dash: tabela com os jogos os jogos.'''),

    dash_table.DataTable(data=df.to_dict('records'), page_size=10),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )

    
])

if __name__ == '__main__':
    app.run(debug=True)
