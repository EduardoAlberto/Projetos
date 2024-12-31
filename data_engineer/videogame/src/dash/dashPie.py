from dash import Dash, html, dcc, dash_table
import plotly.express as px
import pandas as pd
import sqlalchemy
from dbconfig import *


# Configuração da conexão com o banco de dados MySQL
DATABASE_URL = 'mysql+mysqlconnector://root:mysql@localhost:3306/myDbUser?auth_plugin=mysql_native_password'

# Crie um engine de conexão com o banco de dados
engine = sqlalchemy.create_engine(DATABASE_URL)

# Consulta SQL para extrair os dados
query = 'select genres, console, count(*) as total from myDbUser.videogame group by genres, console'

# Leia os dados do banco de dados para um DataFrame do Pandas
df = pd.read_sql(query, engine)

# Inicialize a aplicação Dash
app = Dash(__name__)

# Crie o gráfico de pizza com Plotly Express
fig = px.pie(df, names='genres', values='total', title='Vendas de Video Games por Plataforma')

# Defina o layout do aplicativo
app.layout = html.Div(children=[
    html.H1(children='Video Game Dash'),
    dcc.Graph(
        id='grafico-pizza',
        figure=fig
    )
])

# Execute o servidor da aplicação
if __name__ == '__main__':
    app.run_server(debug=True)
