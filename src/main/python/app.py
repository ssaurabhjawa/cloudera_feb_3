import os
from util import get_spark_session
from read import read_send_message
from read import reading_from_kafka
from transform import transforming_data_from_df
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType



appName = os.environ.get('NAME')
folder_path = os.environ.get('PATH')
env = os.environ.get('ENV')


def main():
    env = os.environ.get('ENVIRON')
    src_dir=os.environ.get('SRC_DIR')
    read_send_message(src_dir)
    spark = get_spark_session(env, appName)
    read_data_from_json = reading_from_kafka(spark)

    for table in read_data_from_json.select('table_name').distinct().toLocalIterator():
        datasets = transforming_data_from_df(read_data_from_json, f'{table.table_name}')
        print(datasets)


if __name__ == '__main__':
    main()
