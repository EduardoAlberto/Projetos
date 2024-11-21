import pandas as pd
import sqlalchemy as mydb

def selectNetflix():
    engine = mydb.create_engine('mysql+mysqlconnector://root:mysql@localhost:3306/myDbUser?auth_plugin=mysql_native_password')
    select = pd.read_sql('select type,country,title from %s limit 10' % 'tb_netflix', engine)
    return select


def selectVideogame():
    engine = mydb.create_engine('mysql+mysqlconnector://root:mysql@localhost:3306/myDbUser?auth_plugin=mysql_native_password')
    select = pd.read_sql('''with temp as (
                            SELECT 
                                genres, 
                                console,
                                publishers,
                                count(*) total 
                            FROM %s group By genres,console,publishers
                        )
                        select * from temp where publishers is not null and total >= 5 ORDER BY total DESC''' % 'videogame', engine)
    return select





# print(df)
