export HADOOP_CONF_DIR="/etc/hadoop/conf"
export SPARK_HOME=/home/itv000925/streaming_venv/lib/python3.6/site-packages/pyspark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
export PATH=$SPARK_HOME/bin:$SPARK_HOME/python:$PATH
export PYSPARK_PYTHON=python3
export ENVIRON=PROD
export SRC_DIR='/home/itv000925/retail_db_data/'
python3 app.py
