from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, lower, sum, desc

if __name__ == "__main__":
    spark = SparkSession.builder.appName('homework-1')\
                .enableHiveSupport()\
                .config( \
                    "spark.jars.packages",\
                    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"\
                )\
                .config(\
                    "spark.sql.extensions",\
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"\
                )\
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")\
                .config("spark.sql.catalog.local.type", "hadoop")\
                .config(\
                    "spark.sql.catalog.local.warehouse",\
                    'file:/Users/wanyisu/Documents/GitHub/data-engineer-handbook/intermediate-bootcamp/materials/3-spark-fundamentals/iceberg_warehouse' \
                )\
                .getOrCreate()

    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    options = {
        "header": "true",
        "inferSchema": "true" 
    }

    match_details = spark.read.options(**options).csv('./data/match_details.csv')
    matches = spark.read.options(**options).csv('./data/matches.csv')
    medals = spark.read.options(**options).csv('./data/medals.csv')
    maps = spark.read.options(**options).csv('./data/maps.csv')
    medals_matches_players = spark.read.options(**options).csv('./data/medals_matches_players.csv')


    ## broadcast small dimension dataframes "maps" and "medals" to join with fact dataframes
    map_matches_df = broadcast(maps).join(matches, on='mapid', how='inner')
    mmp_medals_df = medals_matches_players.join(broadcast(medals), on='medal_id', how='inner')
    final_bc_df = mmp_medals_df.join(map_matches_df, on='match_id', how='inner')


    ## bucket join three tables to reduce shuffling
    # to avoid OOM problem, I use 4 as bucket number instead of 16.
    match_details.write.bucketBy(16, 'match_id')\
        .mode('overwrite')\
        .format('parquet')\
        .sortBy('match_id')\
        .saveAsTable('match_details_bucket_table')
    matches.write.bucketBy(16, 'match_id')\
        .mode('overwrite')\
        .format('parquet')\
        .sortBy('match_id')\
        .saveAsTable('matches_bucket_table')
    medals_matches_players.write.bucketBy(16, 'match_id')\
        .mode('overwrite')\
        .format('parquet')\
        .sortBy('match_id')\
        .saveAsTable('mmp_bucket_table')

    match_details_bucket_df = spark.table('match_details_bucket_table')
    matches_bucket_df = spark.table('matches_bucket_table')
    mmp_bucket_df = spark.table('mmp_bucket_table')

    final_join_df = match_details_bucket_df.alias('df1')\
                        .join(matches_bucket_df, on='match_id', how='inner')\
                        .join(mmp_bucket_df, on=['match_id', 'player_gamertag'], how='inner')\
                        .select('df1.*', 'mapid', 'is_team_game', 'playlist_id', 'game_variant_id', 'is_match_over', 'completion_date'\
                                , 'match_duration', 'game_mode', 'map_variant_id', 'medal_id', 'count')
    
    final_join_df.cache()
    final_join_df.createOrReplaceTempView("final_join_view")


    ## Aggregate the joined dataframes
    # Which player averages the most kills per game?
    match_details.createOrReplaceTempView("match_details_view")
    most_kills_player = spark.sql('''SELECT player_gamertag, avg(player_total_kills) AS avg_kills
                FROM match_details_view
                GROUP BY player_gamertag
                ORDER BY avg(player_total_kills) DESC
                LIMIT 1''')
    most_kills_player.show() # gimpinator14 plays the most average kills at 109.
    
    # Which playlist gets played the most?
    matches.createOrReplaceTempView("matches_view")
    most_playlist = spark.sql('''SELECT playlist_id, COUNT(distinct match_id) AS counts
                              FROM matches_view
                              GROUP BY playlist_id
                              ORDER BY COUNT(distinct match_id) DESC
                              LIMIT 1''')
    most_playlist.show(truncate=False) # playlist f72e0ef0-7c4a-4307-af78-8e38dac3fdba gets play the most by 9350 counts.

    # Which map gets played the most?
    most_map = spark.sql('''SELECT mapid, COUNT(distinct match_id) AS counts
                         FROM final_join_view
                         GROUP BY mapid
                         ORDER BY COUNT(distinct match_id) DESC
                         LIMIT 1''')
    most_map.show(truncate=False) # map c7edbf0f-f206-11e4-aa52-24be05e24f7e gets played the most at 7032 counts.

    # Which map do players get the most Killing Spree medals on?
    kill_spree_medals = medals.where(lower(medals['name']).contains('killing spree')).select('medal_id')
    players_kill_spree = kill_spree_medals.join(medals_matches_players, on='medal_id', how='inner')\
                            .select('match_id', 'count')
    map_kill_spree = players_kill_spree.join(matches.select('match_id', 'mapid'), on='match_id', how='inner')\
                        .groupBy('mapid').agg(sum('count').alias('total_cnt'))\
                        .orderBy(desc('total_cnt'))
    map_kill_spree.show(1, truncate=False) # Mapid c7edbf0f-f206-11e4-aa52-24be05e24f7e has the most Killing Spree medals of 6744.


    ## Try different .sortWithinPartitions()
    # spark.sql('''select count(distinct playlist_id) from final_join_view''').show() # 23 distinct playlist_id
    # spark.sql('''select count(distinct mapid) from final_join_view''').show() # 16 distinct mapid
    # final_join_df.rdd.getNumPartitions() #16  
    # Why use Iceberg/Delta: stores per-file metadata, while parquet/csv/hive/etc don't
    spark.sql('''CREATE NAMESPACE IF NOT EXISTS local.bootcamp''')  
    # When partition by playlist_id:
    spark.sql('''
        CREATE TABLE IF NOT EXISTS local.bootcamp.partition_by_playlist_table_iceberg (
                match_id STRING,
                player_gamertag STRING,
                previous_spartan_rank INTEGER,
                spartan_rank INTEGER,
                previous_total_xp INTEGER,
                total_xp INTEGER,
                previous_csr_tier INTEGER,
                previous_csr_designation INTEGER,
                previous_csr INTEGER,
                previous_csr_percent_to_next_tier INTEGER,
                previous_csr_rank INTEGER,
                current_csr_tier INTEGER,
                current_csr_designation INTEGER,
                current_csr INTEGER,
                current_csr_percent_to_next_tier INTEGER,
                current_csr_rank INTEGER,
                player_rank_on_team INTEGER,
                player_finished BOOLEAN,
                player_average_life STRING,
                player_total_kills INTEGER,
                player_total_headshots INTEGER,
                player_total_weapon_damage DOUBLE,
                player_total_shots_landed INTEGER,
                player_total_melee_kills INTEGER,
                player_total_melee_damage DOUBLE,
                player_total_assassinations INTEGER,
                player_total_ground_pound_kills INTEGER,
                player_total_shoulder_bash_kills INTEGER,
                player_total_grenade_damage DOUBLE,
                player_total_power_weapon_damage DOUBLE,
                player_total_power_weapon_grabs INTEGER,
                player_total_deaths INTEGER,
                player_total_assists INTEGER,
                player_total_grenade_kills INTEGER,
                did_win INTEGER,
                team_id INTEGER,
                is_team_game BOOLEAN,
                playlist_id STRING,
                game_variant_id STRING,
                is_match_over BOOLEAN,
                completion_date TIMESTAMP,
                match_duration STRING,
                game_mode STRING,
                map_variant_id STRING,
                medal_id LONG,
                count INTEGER,
                mapid STRING
                )
        USING iceberg
        PARTITIONED BY (playlist_id)
    ''')

    final_join_df.sortWithinPartitions('completion_date').writeTo("local.bootcamp.partition_by_playlist_table_iceberg").append()

    spark.sql('''
        SELECT SUM(file_size_in_bytes) AS bytes, COUNT(*) AS num_files
        FROM local.bootcamp.partition_by_playlist_table_iceberg.files
    ''').show()
    # The aggregated dataframe sorted within partitions by map columns has 31336918 bytes and 46 num_files.
     
    # When partition by mapid:    
    spark.sql('''
        CREATE TABLE IF NOT EXISTS local.bootcamp.partition_by_map_table_iceberg (
                match_id STRING,
                player_gamertag STRING,
                previous_spartan_rank INTEGER,
                spartan_rank INTEGER,
                previous_total_xp INTEGER,
                total_xp INTEGER,
                previous_csr_tier INTEGER,
                previous_csr_designation INTEGER,
                previous_csr INTEGER,
                previous_csr_percent_to_next_tier INTEGER,
                previous_csr_rank INTEGER,
                current_csr_tier INTEGER,
                current_csr_designation INTEGER,
                current_csr INTEGER,
                current_csr_percent_to_next_tier INTEGER,
                current_csr_rank INTEGER,
                player_rank_on_team INTEGER,
                player_finished BOOLEAN,
                player_average_life STRING,
                player_total_kills INTEGER,
                player_total_headshots INTEGER,
                player_total_weapon_damage DOUBLE,
                player_total_shots_landed INTEGER,
                player_total_melee_kills INTEGER,
                player_total_melee_damage DOUBLE,
                player_total_assassinations INTEGER,
                player_total_ground_pound_kills INTEGER,
                player_total_shoulder_bash_kills INTEGER,
                player_total_grenade_damage DOUBLE,
                player_total_power_weapon_damage DOUBLE,
                player_total_power_weapon_grabs INTEGER,
                player_total_deaths INTEGER,
                player_total_assists INTEGER,
                player_total_grenade_kills INTEGER,
                did_win INTEGER,
                team_id INTEGER,
                is_team_game BOOLEAN,
                playlist_id STRING,
                game_variant_id STRING,
                is_match_over BOOLEAN,
                completion_date TIMESTAMP,
                match_duration STRING,
                game_mode STRING,
                map_variant_id STRING,
                medal_id LONG,
                count INTEGER,
                mapid STRING
                )
        USING iceberg
        PARTITIONED BY (mapid)
    ''')

    final_join_df.sortWithinPartitions('completion_date').writeTo("local.bootcamp.partition_by_map_table_iceberg").append()

    spark.sql('''
        SELECT SUM(file_size_in_bytes) AS bytes, COUNT(*) AS num_files
        FROM local.bootcamp.partition_by_map_table_iceberg.files
    ''').show() 
    # The aggregated dataframe sorted within partitions by map columns has 48872672 bytes and 48 num_files.

    # When partition by (playlist_id, mapid):
    spark.sql('''
        CREATE TABLE IF NOT EXISTS local.bootcamp.partition_by_mp_table_iceberg (
                match_id STRING,
                player_gamertag STRING,
                previous_spartan_rank INTEGER,
                spartan_rank INTEGER,
                previous_total_xp INTEGER,
                total_xp INTEGER,
                previous_csr_tier INTEGER,
                previous_csr_designation INTEGER,
                previous_csr INTEGER,
                previous_csr_percent_to_next_tier INTEGER,
                previous_csr_rank INTEGER,
                current_csr_tier INTEGER,
                current_csr_designation INTEGER,
                current_csr INTEGER,
                current_csr_percent_to_next_tier INTEGER,
                current_csr_rank INTEGER,
                player_rank_on_team INTEGER,
                player_finished BOOLEAN,
                player_average_life STRING,
                player_total_kills INTEGER,
                player_total_headshots INTEGER,
                player_total_weapon_damage DOUBLE,
                player_total_shots_landed INTEGER,
                player_total_melee_kills INTEGER,
                player_total_melee_damage DOUBLE,
                player_total_assassinations INTEGER,
                player_total_ground_pound_kills INTEGER,
                player_total_shoulder_bash_kills INTEGER,
                player_total_grenade_damage DOUBLE,
                player_total_power_weapon_damage DOUBLE,
                player_total_power_weapon_grabs INTEGER,
                player_total_deaths INTEGER,
                player_total_assists INTEGER,
                player_total_grenade_kills INTEGER,
                did_win INTEGER,
                team_id INTEGER,
                is_team_game BOOLEAN,
                playlist_id STRING,
                game_variant_id STRING,
                is_match_over BOOLEAN,
                completion_date TIMESTAMP,
                match_duration STRING,
                game_mode STRING,
                map_variant_id STRING,
                medal_id LONG,
                count INTEGER,
                mapid STRING
                )
        USING iceberg
        PARTITIONED BY (playlist_id, mapid)
    ''')

    final_join_df.sortWithinPartitions('completion_date').writeTo("local.bootcamp.partition_by_mp_table_iceberg").append()

    spark.sql('''
        SELECT SUM(file_size_in_bytes) AS bytes, COUNT(*) AS num_files
        FROM local.bootcamp.partition_by_mp_table_iceberg.files
    ''').show()
    # The aggregated dataframe sorted within partitions by (playlist_id, mapid) columns has 18462646 bytes and 196 num_files.
    # Therefore, the optimal is partitioned by (playlist_id, mapid),

    # unpersist the cached df (remove it from memory and disk)
    final_join_df.unpersist() 