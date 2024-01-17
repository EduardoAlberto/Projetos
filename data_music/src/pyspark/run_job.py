from pyspark.sql import SparkSession,Row,DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

def music(df: DataFrame ) -> DataFrame:
    df = df.selectExpr("cast(acousticness as decimal(3,2)) as acousticness",
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
                              when release_date = '1928' then '1928-01-01' 
                              else release_date
                            end as release_date
                       """,
                       "cast(speechiness as decimal(5,2)) as speechiness",
                       "cast(tempo as decimal(3,2)) as tempo",
                       "cast(valence as decimal(3,2)) as valence",
                       "year")
    
    df = df.withColumn("rowid", monotonically_increasing_id())
    
    return df


def genre(df: DataFrame ) -> DataFrame:
    df = df.selectExpr("cast(danceability as decimal(3,2)) as danceability ",
                       "cast(energy as decimal(3,2)) as energy ",
                       "key ",
                       "cast(loudness as decimal(4,2)) as loudness ",
                       "mode",
                       "cast(speechiness as decimal(3,2)) as speechiness ",
                       "cast(acousticness as decimal(3,2)) as acousticness ",
                       "cast(instrumentalness as decimal(3,2)) as instrumentalness ",
                       "cast(liveness as decimal(3,2)) as liveness ",
                       "cast(valence as decimal(3,2)) as valence ",
                       "cast(tempo as decimal(3,2)) as tempo ",
                       "type",
                       "uri",
                       "track_href",
                       "analysis_url",
                       "duration_ms",
                       "time_signature",
                       "genre",
                       "song_name")
    df = df.withColumn("rowid", monotonically_increasing_id())

    return df