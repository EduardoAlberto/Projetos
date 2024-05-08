from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class runJob():

    def __init__(self,music,genres,artists,playlists,tracks):
        self.__txt01  = music
        self.__txt02  = genres
        self.__txt03  = artists
        self.__txt04  = playlists
        self.__txt05  = tracks
    
    def music(self):

        df_music =  self.__txt01.selectExpr("cast(acousticness as decimal(3,2)) as acousticness",
                                            "regexp_replace(artists,'[^a-zA-Z0-9]',' ') as artists",
                                            "cast(danceability as decimal(3,2)) as danceability ",
                                            "duration_ms",
                                            "cast(energy as decimal(3,2)) as energy",
                                            "explicit",
                                            "cast(instrumentalness as decimal(3,2) ) as instrumentalness",
                                            "key",
                                            "cast (liveness as decimal(3,2) ) as liveness",
                                            "mode",
                                            "cast(loudness as decimal(3,2)) as loudness",
                                            "name",
                                            "popularity",
                                            """ case
                                                    when release_date in ('1928','2013') then '1928-01-01' 
                                                    else release_date
                                                end as release_date
                                            """,
                                            "cast(speechiness as decimal(5,2)) as speechiness",
                                            "cast(tempo as decimal(3,2)) as tempo",
                                            "cast(valence as decimal(3,2)) as valence",
                                            "year"
        )
        df_music = df_music.withColumn("rowid", monotonically_increasing_id())
        return df_music
    
    def genre(self):
        df_genre = self.__txt02.selectExpr("cast(acousticness as decimal(3,2)) as acousticness",
                               "regexp_replace(artists,'[^a-zA-Z0-9]',' ') as artists",
                               "cast(danceability as decimal(3,2)) as danceability ",
                               "cast(energy as decimal(3,2)) as energy",
                               "duration_ms",
                               "explicit",
                               "id",
                               "cast(instrumentalness as decimal(3,2) ) as instrumentalness",
                               "key",
                               "cast (liveness as decimal(3,2) ) as liveness",
                               "cast(loudness as decimal(3,2)) as loudness",
                               "mode",
                               "name",
                               "popularity",
                               """ case
                                        when release_date in ('1928','2013') then '1928-01-01' 
                                        else release_date
                                    end as release_date
                               """,
                               "cast(speechiness as decimal(5,2)) as speechiness",
                               "cast(tempo as decimal(3,2)) as tempo",
                               "cast(valence as decimal(3,2)) as valence",
                               "year"
                               )\
                   .withColumn('dt_release_date',expr("case when release_date = 'Whipped into Shape' then '1990-01-10' else release_date end"))\
                   .withColumn('danceability_', expr(" case when trim(regexp_replace(danceability,'[^a-zA-Z0-9]',' ')) = 'Michael Rupert' then 0 else danceability end  "))\
                   .withColumn("rowid", monotonically_increasing_id()) 

        return df_genre

    def  artists(self):
        
        df_artists = self.__txt03.selectExpr("followers",
                                             "regexp_replace(genres,'[^a-zA-Z0-9]',' ') as genres",
                                             "name",
                                             "popularity"
                        

        )
        df_artists = df_artists.withColumn("id", row_number().over(Window.partitionBy("name").orderBy("name")))
        df_artists = df_artists.selectExpr("id",
                                           "trim(genres)",
                                           "trim(regexp_replace(name, '[^a-zA-Z0-9]',' ') ) as name",
                                           "trim(regexp_replace(popularity, '[^a-zA-Z0-9]',' ') ) as popularity",
                                           "followers"
        )
        df_artists = df_artists.withColumn("rowid", monotonically_increasing_id())

        return df_artists

    def playlists(self):
        df_playlists = self.__txt04.select("Playlist","Genre")
        df_playlists = df_playlists.withColumn("rowid", monotonically_increasing_id())

        return df_playlists


    def tracks(self):
        df_tracks = self.__txt05.selectExpr("id"
                                           ,"name"
                                           ,"popularity"
                                           ,"duration_ms"
                                           ,"explicit"
                                           ,"regexp_replace('artists','[^a-zA-Z0-9]',' ') as artists "
                                           ,"regexp_replace('id_artists','[^a-zA-Z0-9]',' ') as id_artists"

        )
        return df_tracks

    