from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
import os
from pyflink.table.window import Session
from pyflink.table.expressions import lit, col

def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")

    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_aggregated_events_sink_postgres(t_env):
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            events_in_session BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_session_events_sink_postgres(t_env, table_name):
    sink_ddl = f'''
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_events_in_session DECIMAL(10, 2),
            PRIMARY KEY (host) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    '''
    t_env.execute_sql(sink_ddl)
    return table_name

def average_sql_calculation(source_table, sink_table, pattern):
    sql_query = f'''INSERT INTO {sink_table}
            SELECT host, ROUND(AVG(events_in_session), 2) AS avg_events_in_session
            FROM {source_table}
            WHERE host LIKE '{pattern}'
            GROUP BY host
            '''
    return sql_query
    
def log_aggregation():
    print('Starting Job!')

    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    print('got streaming environment')
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    ss = t_env.create_statement_set()

    try:
        # source table in kafka is just a logical table, no real data inside (we didn't insert data into it)
        source_table = create_processed_events_source_kafka(t_env)
        aggregated_table = create_aggregated_events_sink_postgres(t_env)
        session_events_table_tc = create_session_events_sink_postgres(t_env, 'processed_session_events_aggregated_tc')
        session_events_table_zachtc = create_session_events_sink_postgres(t_env, 'processed_session_events_aggregated_zachtc')
        session_events_table_zacht = create_session_events_sink_postgres(t_env, 'processed_session_events_aggregated_zacht')
        session_events_table_lulu = create_session_events_sink_postgres(t_env, 'processed_session_events_aggregated_lulu')
        
        # "Use 5-minute gap": session window is 5-minute, not Tumble window.
        # Q1: sessionizes the input data by IP address and host
        t = t_env.from_path(source_table)\
            .window(
                Session.with_gap(lit(1).minutes).on(col('window_timestamp')).alias('w')
            )\
            .group_by(
                col('w'),
                col('ip'),
                col('host')
            )\
            .select(
                col("w").start.alias("session_start"),
                col('w').end.alias('session_end'),
                col('ip'),
                col("host"),
                lit(1).count.alias("events_in_session")
            )
            # .execute_insert(aggregated_table)
        t_env.create_temporary_view("agg_view", t)
        ss.add_insert(aggregated_table, t)

        ## Q2: average number of web events of a session from a user on Tech Creator
        sql_tc = f'''INSERT INTO {session_events_table_tc}
            SELECT host, ROUND(AVG(events_in_session), 2) AS avg_events_in_session
            FROM agg_view
            WHERE host = 'techcreator.io'
            GROUP BY host
            '''
        ss.add_insert_sql(sql_tc)

        ## Q3: Comparison
        sql_zachtc = average_sql_calculation("agg_view", session_events_table_zachtc, 'zachwilson.techcreator.io')
        ss.add_insert_sql(sql_zachtc)
        sql_zacht = average_sql_calculation("agg_view", session_events_table_zacht, 'zachwilson.tech')
        ss.add_insert_sql(sql_zacht)
        sql_lulu = average_sql_calculation("agg_view", session_events_table_lulu, 'lulu.techcreator.io')
        ss.add_insert_sql(sql_lulu)
            
        ss.execute()
    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_aggregation()