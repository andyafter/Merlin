FROM ocassetti/spark-py:2.4.5.2

USER root

# Add the connector jar needed to access Google Cloud Storage using the Hadoop FileSystem API.
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar $SPARK_HOME/jars
ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest.jar $SPARK_HOME/jars

# Force python3
ENV PYSPARK_PYTHON python3
ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-*.zip:/opt/spark/work-dir/

ADD main.py /opt/spark/work-dir/
ADD entry.py /opt/spark/work-dir/
ADD merlin /opt/spark/work-dir/merlin
