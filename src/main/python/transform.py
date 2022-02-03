import os
from util import get_spark_session

appName = os.environ.get('NAME')
env = os.environ.get('ENV')


def transforming_data_from_df(df, table_name):
    spark = get_spark_session(env, appName)
    records = df. \
        filter(f'table_name = "{table_name}"'). \
        select('record')
    transformed_df = spark.read.json(records.select('record').rdd.map(lambda item: item.record))
    return transformed_df

