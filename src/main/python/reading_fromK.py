from pyspark.sql import functions as F
import os
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import *


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
