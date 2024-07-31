from pyspark.sql import SparkSession,Row,DataFrame
from run_etl import RunJob
from pyspark.sql.types import *
import os.path as path

# Create SparkSession
spark=SparkSession.builder.master("local[2]")\
                          .appName("Music")\
                          .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mysql-connector-j-8.2.0.jar" ) \
                          .getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("OFF") 
print('PySpark Version :'+spark.version)
print('PySpark Version :'+spark.sparkContext.version)

print(50*'#')
print("Inicio do processo")
print(50*'#')


music       = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/data.csv')    
genre       = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/genres_v2.csv')
artists     = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/artists.csv')
playlists   = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/playlists.csv')
tracks      = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/tracks.csv')

x = RunJob(music,genre,artists,playlists,tracks)
df_music     = x.music()
df_genre     = x.genre()
df_artists   = x.artists()
df_playlists = x.playlists()
df_tracks    = x.tracks()



# df_music.write.format("delta").mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.tb_music",properties={"user": "root", "password": "mysql"})
# df_tracks.write.format("delta").mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.tb_tracks",properties={"user": "root", "password": "mysql"})
# df_genres.write.format("delta").mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.tb_genres",properties={"user": "root", "password": "mysql"})
# df_artists.write.format("delta").mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.tb_artists",properties={"user": "root", "password": "mysql"})
# df_playlists.write.format("delta").mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.tb_playlists",properties={"user": "root", "password": "mysql"})

print(50*'#')
print("Fim do processo")
print('Total de registros  music:' + str(df_music.count()))
print('Total de registros  genero:' + str(df_genre.count()))
print('Total de registros  artista:' + str(df_artists.count()))
print('Total de registros  playlists:' + str(df_playlists.count()))
print('Total de registros  tracks:' + str(df_tracks.count()))
print(50*'#')

