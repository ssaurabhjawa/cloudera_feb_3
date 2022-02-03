from pyspark.sql import functions as F
import os
from confluent_kafka import Producer
import time
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import *


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def read_send_message(src_dir):
    conf = {'bootstrap.servers': 'w01.itversity.com:9092,w02.itversity.com:9092'}
    p = Producer(conf)
    for entry in os.listdir(src_dir):
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)
        file_dir = os.path.join(src_dir, entry)
        for filesInDir in os.listdir(file_dir):
            table_folder = os.path.join(file_dir, filesInDir)
            retail_db_files = open(table_folder)
            for line in retail_db_files:
                # Asynchronously produce a message, the delivery report callback
                # will be triggered from poll() above, or flush() below, when the message has
                # been successfully delivered or failed permanently.
                p.produce('retail_topic_1', key="key", value=line, callback=delivery_report)
                time.sleep(1)

            # Wait for any outstanding messages to be delivered and delivery report
            # callbacks to be triggered.
            p.flush()

def reading_from_kafka(spark):
    kafka_bootstrap_servers = 'w01.itversity.com:9092,w02.itversity.com:9092'

    #read data from Kafka Topic
    df = spark. \
      readStream. \
      format('kafka'). \
      option('kafka.bootstrap.servers', kafka_bootstrap_servers). \
      option('subscribe', 'retail_db'). \
      option("startingOffsets","earliest"). \
      load()
    schema = StructType(
            [
                    StructField("table_name", StringType()),
                    StructField("record", StringType())
            ]
    )

    #Extracting dataFrame with columns, table name and record
    df_value = df.select(col('value').cast('string'))
    write_df = df_value. \
        withColumn("value", from_json("value", schema)). \
        writeStream. \
        format("memory"). \
        queryName("retail_poc_3"). \
        start()

    df_qn = spark.sql('SELECT value FROM retail_poc_3')
    df_table = df_qn.select(col('value')['table_name'].alias('table_name'), col('value')['record'].alias('record'))
    return df_table


