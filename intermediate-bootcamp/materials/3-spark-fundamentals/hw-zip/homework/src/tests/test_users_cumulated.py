from chispa.dataframe_comparer import *
from ..jobs.users_cumulated_job import do_users_cumulated
from collections import namedtuple
from pyspark.sql import types as T
from datetime import datetime

usersCumulated = namedtuple('users_cumulated', 'user_id dates_active date')
Events = namedtuple('events', 'user_id device_id url referrer host event_time')

users_cumulated_schema = T.StructType([
    T.StructField("user_id", T.IntegerType()),
    T.StructField("dates_active", T.ArrayType(T.DateType())),
    T.StructField("date", T.DateType())
])
events_schema = T.StructType([
    T.StructField("user_id", T.IntegerType()),
    T.StructField("device_id", T.IntegerType()),
    T.StructField("url", T.StringType()),
    T.StructField("referrer", T.StringType()),
    T.StructField("host", T.StringType()),
    T.StructField("event_time", T.TimestampType())
])

def test_users_cumulated(spark):
    td = '2025-01-03'

    source_data_users_cumulated = [
        usersCumulated(
            user_id=11101, 
            dates_active=[datetime.strptime('2025-01-01', '%Y-%m-%d'), datetime.strptime('2025-01-02', '%Y-%m-%d')],
            date=datetime.strptime('2025-01-02', '%Y-%m-%d'))
    ]
    source_users_cumu_df = spark.createDataFrame(source_data_users_cumulated, schema=users_cumulated_schema)

    source_data_events = [
        Events(user_id=11101, device_id=22201, url='https://google.com', referrer='LinkedIn', host='google', event_time=datetime.strptime('2025-01-03 12:09', '%Y-%m-%d %M:%S')),
        Events(user_id=11101, device_id=22201, url='https://google.com', referrer='LinkedIn', host='google', event_time=datetime.strptime('2025-01-04 09:08', '%Y-%m-%d %M:%S')),
        Events(user_id=11102, device_id=22202, url='https://apple.com', referrer='Indeed', host='apple', event_time=datetime.strptime('2025-01-03 16:00', '%Y-%m-%d %M:%S')),
    ]
    source_events_df = spark.createDataFrame(source_data_events, schema=events_schema)
    actual_df = do_users_cumulated(spark, source_users_cumu_df, source_events_df, td)

    expected_data = [
        usersCumulated(
            user_id=11101,
            dates_active=[datetime.strptime('2025-01-01', '%Y-%m-%d'), datetime.strptime('2025-01-02', '%Y-%m-%d'), datetime.strptime('2025-01-03', '%Y-%m-%d')],
            date=datetime.strptime('2025-01-03', '%Y-%m-%d')
        ),
        usersCumulated(
            user_id=11102,
            dates_active=[datetime.strptime('2025-01-03', '%Y-%m-%d')],
            date=datetime.strptime('2025-01-03', '%Y-%m-%d')
        )
    ]
    expected_df = spark.createDataFrame(expected_data, schema=users_cumulated_schema)
    actual_df.show()
    expected_df.show()
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)