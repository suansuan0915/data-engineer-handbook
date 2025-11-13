from chispa.dataframe_comparer import *
from ..jobs.users_cumulated_job import do_users_cumulated
from collections import namedtuple
from pyspark.sql import types as T

usersCumulated = namedtuple('users_cumulated', 'user_id date_list date')
Events = namedtuple('events', 'user_id device_id url referrer host event_time')

def test_users_cumulated(spark):
    source_data_users_cumulated = [
        usersCumulated(),
    ]