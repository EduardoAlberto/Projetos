import pandas as pd
import pyodbc


# conexao com o Mssql
server = 'localhost'
database = 'DBDWP511'
username = 'sa'
password = 'Numsey@Password!'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# query 
query = pd.read_sql_query("""SELECT * FROM DBDWP511.DBO.VW_ANALISIE""",cnxn)

print(query)