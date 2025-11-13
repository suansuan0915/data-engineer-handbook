from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

if __name__ == "__main__":
    spark = SparkSession.builder\
            .appName('homework-1')\
            .getOrCreate()

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    match_details = spark.read.options(header=True).csv('../data/match_details.csv')
    matches = spark.read.options(header=True).csv('../data/matches.csv')
    medals = spark.read.options(header=True).csv('../data/medals.csv')
    maps = spark.read.options(header=True).csv('../data/maps.csv')
    medals_matches_players = spark.read.options(header=True).csv('../data/medals_matches_players.csv')

    ## broadcast small dimension dataframe "maps" to join with fact dataframe "medals"
    bc_join_df = broadcast(maps).join(medals, how='cross')

    ## bucket join three tables to reduce shuffling
    # to avoid OOM problem, I use 4 as bucket number instead of 16.
    match_details.write.bucketBy(8, 'match_id').mode('overwrite').saveAsTable('match_details_bucket_table')
    matches.write.bucketBy(8, 'match_id').mode('overwrite').saveAsTable('matches_bucket_table')
    medals_matches_players.write.bucketBy(8, 'match_id').mode('overwrite').saveAsTable('medal_matches_players_bucket_table')

    match_details_bucket_df = spark.table('match_details_bucket_table')
    matches_bucket_df = spark.table('matches_bucket_table')
    medal_matches_players_bucket_df = spark.table('medal_matches_players_bucket_table')

    final_join_df = match_details_bucket_df.alias('df1')\
                        .join(matches_bucket_df, on='match_id', how='inner')\
                        .join(medal_matches_players_bucket_df, on='match_id', how='inner')\
                        .select('df1.*', 'mapid', 'is_team_game', 'playlist_id', 'game_variant_id', 'is_match_over', 'completion_date'\
                                , 'match_duration', 'game_mode', 'map_variant_id', 'medal_id', 'count')
    
    final_join_df.cache()
    final_join_df.createOrReplaceTempView("final_join_view")

    ## Aggregate the joined dataframes
    # Which player averages the most kills per game?
    most_kills_player = spark.sql('''SELECT player_gamertag, avg(player_total_kills) AS avg_kills
              FROM final_join_view
              GROUP BY player_gamertag
              ORDER BY avg(player_total_kills) DESC
              LIMIT 1''')
    most_kills_player.show() # gimpinator14 plays the most average kills at 109.
    
    # Which playlist gets played the most?
    most_playlist = spark.sql('''SELECT playlist_id, COUNT(1) AS counts
                              FROM final_join_view
                              GROUP BY playlist_id
                              ORDER BY COUNT(1) DESC
                              LIMIT 1''')
    most_playlist.show(truncate=False) # playlist f72e0ef0-7c4a-4307-af78-8e38dac3fdba gets play the most by 1565529 counts.

    # Which map gets played the most?
    most_map = spark.sql('''SELECT mapid, COUNT(1) AS counts
                         FROM final_join_view
                         GROUP BY mapid
                         ORDER BY COUNT(1) DESC
                         LIMIT 1''')
    most_map.show(truncate=False) # map c74c9d0f-f206-11e4-8330-24be05e24f7e gets played the most at 1445545 counts.

    # Which map do players get the most Killing Spree medals on?
    medals.createOrReplaceTempView("medals_view")
    most_spree_medal = spark.sql('''SELECT mapid, SUM(count) AS counts
              FROM final_join_view t1 INNER JOIN medals_view t2
              ON t1.medal_id = t2.medal_id
              WHERE LOWER(name) LIKE '%killing spree%'
              GROUP BY mapid
              ORDER BY SUM(count) DESC
              LIMIT 1''')
    most_spree_medal.show(truncate=False) # Mapid c74c9d0f-f206-11e4-8330-24be05e24f7e has the most Killing Spree medals of 71863.

    ## Try different .sortWithinPartitions()
    # spark.sql('''select count(distinct playlist_id) from final_join_view''').show() # 23 distinct playlist_id
    # spark.sql('''select count(distinct mapid) from final_join_view''').show() # 16 distinct mapid
    # final_join_df.rdd.getNumPartitions() #16
    # sort_playlist_df = final_join_df.sortWithinPartitions(col('playlist_id'))
    # sort_playlist_df.write.mode('overwrite').saveAsTable('sort_playlist_table')
    # sort_playlist_df.createOrReplaceTempView('sort_playlist_view')
    # spark.sql('''SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_by_playlist' 
    #           FROM sort_playlist_view.files''')
    # sort_mapid_df = final_join_df.sortWithinPartitions(col('mapid'))
    # sort_mapid_df.write.mode('overwrite').saveAsTable('sort_mapid_table')
    # sort_mapid_df.createOrReplaceTempView('sort_mapid_view')
    # spark.sql('''SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_by_map' 
    #           FROM sort_mapid_view.files''')
    sort_map_playlist_df = final_join_df.sortWithinPartitions(col('mapid'), col('playlist_id'))
    sort_map_playlist_df.write.mode('overwrite').saveAsTable('sort_map_playlist_table')
    sort_map_playlist_df.createOrReplaceTempView('sort_map_playlist_view')
    spark.sql('''SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_by_map_playlist' 
              FROM sort_map_playlist_view.files''')
    # The aggregated dataframe sorted within partitions by map and playlist columns (which have very low cardinality).