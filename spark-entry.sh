spark-submit \
--jars $SPARK_HOME/jars/spark-bigquery-latest.jar,$SPARK_HOME/jars/gcs-connector-latest-hadoop2.jar \
main.py \
--metric_db ../airline/stg/metrics.yaml \
--sql_archive ../metric_sql.zip \
--dt_start 1 \
--dt_end 2 \
--engine big_query \
--keyfile /home/andy/thefuck/airasia-opdatalake-stg-terraform.json \
--configs ../config.yml \
--gcs_temp_bucket airasia-metrics-data-stg