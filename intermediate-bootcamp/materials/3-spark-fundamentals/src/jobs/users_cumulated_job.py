from pyspark.sql import SparkSession

query = f'''
    WITH yesterday AS (
        SELECT * FROM users_cumulated
        WHERE date = DATE('2023-03-30')
    ),
        today AS (
            SELECT user_id,
                    DATE_TRUNC('day', event_time) AS today_date,
                    COUNT(1) AS num_events 
            FROM events
            WHERE DATE_TRUNC('day', event_time) = DATE('2023-03-31')
                AND user_id IS NOT NULL
            GROUP BY user_id,  DATE_TRUNC('day', event_time)
        )

    SELECT
        COALESCE(t.user_id, y.user_id) AS user_id,
        COALESCE(y.dates_active,
            ARRAY[]::DATE[])
                || CASE WHEN
                    t.user_id IS NOT NULL
                    THEN ARRAY[t.today_date]
                    ELSE ARRAY[]::DATE[]
                    END AS date_list,
        COALESCE(t.today_date, y.date + Interval '1 day') as date
    FROM yesterday y
        FULL OUTER JOIN
        today t ON t.user_id = y.user_id;
    '''

def do_users_cumukated(spark, dataframe_1, dataframe_2):
    dataframe_1.createOrReplaceTempView('users_cumulated')
    dataframe_2.createOrReplaceTempView('events')
    return spark.sql(query)

def main():
    spark = SparkSession.builder\
        .master('local')\
        .appName('users_cumulated')\
        .getOrCreate()
    
    output_df = do_users_cumukated(spark, spark.table('users_cumulated'), spark.table('events'))
    output_df.write.mode('overwrite').insertInto('users_cumulated_table')