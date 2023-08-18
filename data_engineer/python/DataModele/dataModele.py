
import pyodbc
from pyodbc import Error
import pandas as pd


class msqlserver:
    # variaveis de entrada
    def __init__(self,server, database, uid, pwd, query):
        self.__server = server
        self.__database = database
        self.__uid = uid
        self.__pwd = pwd
        self.__query = query
   

    # Função de connect 
    def create_server_connection(self):
        connection = None
        try:
            connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.__server+';DATABASE='+self.__database+';UID='+self.__uid+';PWD='+ self.__pwd,autocommit=True)
            print(20*'#')
            print("Conectado no Banco SQLSERVER")
        except Error as err:
            print(f"Error: '{err}'")

        return connection
    # create database
    def create_database(self,connection):
        cursor = connection.cursor()
        db = self.__query[16:]
        try:
            cursor.execute(f'''USE master;
                               DROP DATABASE {db};''')
            cursor.execute(self.__query)

            print("Database criado com sucesso! ")
            print(20*'#')
        except Error as err:
            print(f"Error: '{err}'") 