from dataModele import msqlserver


query = 'CREATE DATABASE DBPRODASH '

# execute DataModel.py

x = msqlserver('localhost','DBDWP511','sa','Numsey@Password!',query)
connection = x.create_server_connection()
x.create_database(connection)
