import os
from util import get_spark_session
from transform import transforming_data_from_json
from read import reading_json_file
from read import read_send_message

appName = os.environ.get('NAME')
folder_path = os.environ.get('PATH')
env = os.environ.get('ENV')


def main():
    env = os.environ.get('ENVIRON')
    src_dir=os.environ.get('SRC_DIR')
    read_send_message(src_dir)
    spark = get_spark_session(env, appName)
    read_data_from_json = reading_json_file(spark, folder_path)
    for table in read_data_from_json.select('table_name').distinct().toLocalIterator():
        datasets = transforming_data_from_json(read_data_from_json, f'{table.table_name}')
        return datasets


if __name__ == '__main__':
    main()
