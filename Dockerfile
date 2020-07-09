FROM gcr.io/spark-operator/spark-py:v2.4.5

USER root
RUN rm $SPARK_HOME/jars/guava-14.0.1.jar
ADD https://repo1.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar $SPARK_HOME/jars

# Add the connector jar needed to access Google Cloud Storage using the Hadoop FileSystem API.
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar $SPARK_HOME/jars
ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest.jar $SPARK_HOME/jars

RUN rm $SPARK_HOME/jars/spark-kubernetes_2.11-2.4.5.jar
ADD spark-kubernetes_2.11-2.4.5.jar $SPARK_HOME/jars

ADD python_packages.txt python_packages.txt
RUN pip3 install -r requirements.txt
RUN rm python_packages.txt
# Force python3
ENV PYSPARK_PYTHON python3

# ENTRYPOINT ["/opt/entrypoint.sh"]
