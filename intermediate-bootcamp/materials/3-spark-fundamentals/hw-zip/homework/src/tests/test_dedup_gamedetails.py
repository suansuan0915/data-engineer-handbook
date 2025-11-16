from chispa.dataframe_comparer import *
from ..jobs.dedup_gamedetails_job import do_dedup_gamedetails
from collections import namedtuple
from pyspark.sql import types as T

gameDetails = namedtuple('gameDetails', \
    "game_id team_id team_abbreviation team_city player_id player_name \
    nickname start_position comment min \
    fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct \
    oreb dreb reb ast stl blk TO pf pts plus_minus" \
)

schema = T.StructType([
    T.StructField("game_id", T.IntegerType()),
    T.StructField("team_id", T.IntegerType()),
    T.StructField("team_abbreviation", T.StringType()),
    T.StructField("team_city", T.StringType()),
    T.StructField("player_id", T.IntegerType()),
    T.StructField("player_name", T.StringType()),
    T.StructField("nickname", T.StringType()),
    T.StructField("start_position", T.StringType()),
    T.StructField("comment", T.StringType()),
    T.StructField("min", T.StringType()),      
    T.StructField("fgm", T.DoubleType()),
    T.StructField("fga", T.DoubleType()),
    T.StructField("fg_pct", T.DoubleType()),
    T.StructField("fg3m", T.DoubleType()),
    T.StructField("fg3a", T.DoubleType()),
    T.StructField("fg3_pct", T.DoubleType()),
    T.StructField("ftm", T.DoubleType()),
    T.StructField("fta", T.DoubleType()),
    T.StructField("ft_pct", T.DoubleType()),
    T.StructField("oreb", T.DoubleType()),
    T.StructField("dreb", T.DoubleType()),
    T.StructField("reb", T.DoubleType()),
    T.StructField("ast", T.DoubleType()),
    T.StructField("stl", T.DoubleType()),
    T.StructField("blk", T.DoubleType()),
    T.StructField("TO",  T.DoubleType()),         
    T.StructField("pf",  T.DoubleType()),
    T.StructField("pts", T.DoubleType()),
    T.StructField("plus_minus", T.DoubleType()),
])

def test_dedup_gd(spark):
    source_data = [ 
        gameDetails(11600001, 1610612744, 'GSW', 'Golden State', 2561, 'David West', 'DW', 'C', 'no comment', '12:36', 2.0, 5.0,0.4,1.0,2.0,0.5,1.0,2.0,0.5,1.0,5.0,6.0,0.0,1.0,1.0,3.0,3.0,6.0,9.0),
        gameDetails(11600001, 1610612744, 'GSW', 'Golden State', 2561, 'David West', 'DW', 'C', 'no comment', '12:36', 2.0, 5.0,0.4,1.0,2.0,0.5,1.0,2.0,0.5,1.0,5.0,6.0,0.0,1.0,1.0,3.0,3.0,6.0,9.0),
        gameDetails(11600003, 1610612745, 'ABC', 'Golden State', 2780, 'David East', 'CD', 'A', 'cmt', '14:36', 2.0, 5.0,0.4,6.0,2.0,0.5,1.0,2.0,0.9,1.0,3.0,6.0,0.0,1.0,1.0,3.0,9.0,6.0,8.0),
    ]
    source_df = spark.createDataFrame(source_data, schema=schema)
    actual_df = do_dedup_gamedetails(spark, source_df)

    expected_data = [
        gameDetails(11600001, 1610612744, 'GSW', 'Golden State', 2561, 'David West', 'DW', 'C', 'no comment', '12:36', 2.0, 5.0,0.4,1.0,2.0,0.5,1.0,2.0,0.5,1.0,5.0,6.0,0.0,1.0,1.0,3.0,3.0,6.0,9.0),
        gameDetails(11600003, 1610612745, 'ABC', 'Golden State', 2780, 'David East', 'CD', 'A', 'cmt', '14:36', 2.0, 5.0,0.4,6.0,2.0,0.5,1.0,2.0,0.9,1.0,3.0,6.0,0.0,1.0,1.0,3.0,9.0,6.0,8.0)
    ]
    expected_df = spark.createDataFrame(expected_data, schema=schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)