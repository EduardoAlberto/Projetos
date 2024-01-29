from pyspark.sql import SparkSession,Row,DataFrame
from run_etl import runJob

# Create SparkSession
spark=SparkSession.builder.master("local[2]")\
                          .appName("Music")\
                          .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mysql-connector-j-8.2.0.jar" ) \
                          .getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("OFF") 
print('PySpark Version :'+spark.version)
print('PySpark Version :'+spark.sparkContext.version)

music = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/data.csv')
tracks = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/tracks.csv')
genres = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/genres_v2.csv')
artists = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/artists.csv')
playlists = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/archive/playlists.csv')

x = runJob(music,genres,artists,playlists,tracks)
df_music  = x.music()
df_tracks = x.tracks()
df_genres = x.genre()
df_artists = x.artists()
df_playlists = x.playlists()

df_tracks.show()  