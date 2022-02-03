from confluent_kafka import Consumer
from util import get_spark_session
from pyspark.sql.functions import date_format, to_date, split, substring
import getpass


def kafka_consumer(env, appName):
    conf = {'bootstrap.servers': 'w01.itversity.com:9092,w02.itversity.com:9092'}
    c = Consumer(conf)

    c.subscribe(['retail_db'])
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))
    c.close()

