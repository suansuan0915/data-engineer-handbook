from pyspark.sql import SparkSession

def do_users_cumulated(spark, dataframe_1, dataframe_2, td):
    query = f'''
        WITH yesterday AS (
            SELECT * 
            FROM users_cumulated 
            WHERE date = date_add('{td}', -1)
        ),
            today AS (
                SELECT user_id,
                        DATE_TRUNC('day', event_time) AS today_date,
                        COUNT(1) AS num_events 
                FROM events
                WHERE DATE_TRUNC('day', event_time) = '{td}'
                    AND user_id IS NOT NULL
                GROUP BY user_id,  DATE_TRUNC('day', event_time)
            )

        SELECT
            COALESCE(t.user_id, y.user_id) AS user_id,
            CAST(COALESCE(y.dates_active,
                CAST(array() AS array<date>)) AS array<date>)
                    || CASE WHEN
                        t.user_id IS NOT NULL
                        THEN CAST(ARRAY(t.today_date) AS array<date>)
                        ELSE CAST(array() AS array<date>)
                        END AS dates_active,
            CAST(COALESCE(t.today_date, y.date + Interval '1 day') AS date) as date
        FROM yesterday y
            FULL OUTER JOIN
            today t ON t.user_id = y.user_id;
    '''

    dataframe_1.createOrReplaceTempView('users_cumulated')
    dataframe_2.createOrReplaceTempView('events')
    return spark.sql(query)

def main():
    td = '2025-01-03'
    spark = SparkSession.builder\
        .master('local')\
        .appName('users_cumulated')\
        .getOrCreate()
    
    output_df = do_users_cumulated(spark, spark.table('users_cumulated'), spark.table('events'), td)
    output_df.write.mode('overwrite').insertInto('users_cumulated_table')