import pandas as pd
import database as bd
import plotly.graph_objects as go 
import plotly.offline as py
import plotly.express as px 
from dash import Dash
from  dash_html_components import Div, H1
from  dash_core_components import Graph

# query que vai alimentar o dash
t1 = pd.read_sql_query("""SELECT * FROM DBDWP511.DBO.VW_ANALISIE""",bd.cnxn)
app = Dash()


# grafico tabela
fig = go.Figure(data=[go.Table(header=dict(values=['FirstName','BirthDate','SalesAmount']),
            cells=dict(values=[t1['FirstName'], 
                                t1['BirthDate'],
                                t1['SalesAmount']]
                                ))
                ])

app.layout = Div([
        # Titulo
        H1("Tabela da Empresa e Funcionarios ",
                style = {'textAlign': 'center', # alinhando o titulo ao centro
                         'fontFamily': 'Roboto', # alterando a fonte do H1
                         'paddingTop': 18}), # adicionando um padding no topo

        # plot da tabela 
        Graph(
            id="tabela",
            figure=fig,
    )
    
])

if __name__ == '__main__':
    app.run_server(debug = True, use_reloader = True)
