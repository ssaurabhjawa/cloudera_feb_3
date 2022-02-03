from confluent_kafka import Consumer
from util import get_spark_session
from pyspark.sql.functions import date_format, to_date, split, substring
import getpass
from util import get_config


def kafka_consumer(env, appName):
    configs = get_config()
    conf = {configs.keys(): configs.values(),
            'group.id': "retail",
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest'}

    c = Consumer(conf)

    c.subscribe(['retail_topic_1'])
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))
    c.close()

