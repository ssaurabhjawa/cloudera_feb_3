from pyspark.sql import SparkSession
import getpass


def get_spark_session(env, appName):
    username = getpass.getuser()
    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'). \
            config('spark.ui.port', '0'). \
            config('spark.sql.warehouse.dir', f'/user/{username}/warehouse'). \
            enableHiveSupport(). \
            appName(f'{username} | Python - Kafka and Spark Integration'). \
            master('local'). \
            getOrCreate()
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'). \
            config('spark.ui.port', '0'). \
            config('spark.sql.warehouse.dir', f'/user/{username}/warehouse'). \
            enableHiveSupport(). \
            appName(f'{username} | Python - Kafka and Spark Integration'). \
            master('yarn'). \
            getOrCreate()
        return spark
    return

