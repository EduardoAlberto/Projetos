import mysql.connector
from mysql.connector import Error

try:
    # Estabelecer a conexão
    connection = mysql.connector.connect(user='root',password='mysql',host='localhost',database='myDbUser')
                
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Conectado ao servidor MySQL versão ", db_Info)
        
        # Criar um cursor
        cursor = connection.cursor()
        
        # Selecionar o banco de dados atual
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Conectado ao banco de dados: ", record)

        # Executar uma consulta
        cursor.execute("SELECT * FROM videogame;")
        
        # Obter todos os resultados
        result = cursor.fetchall()
        
        # Processar os resultados
        for row in result:
            print(row)

except Error as e:
    print("Erro ao conectar ao MySQL ou executar a consulta", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Conexão ao MySQL foi encerrada")