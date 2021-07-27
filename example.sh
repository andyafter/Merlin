/opt/spark/bin/spark-submit \
--jars /home/andy/workspace/spark-k8s/jars/spark-bigquery-latest.jar,/home/andy/workspace/spark-k8s/jars/gcs-connector-hadoop2-2.0.1-shaded.jar \
main.py \
--metric_db ../airline/stg/metrics.yaml \
--sql_archive ../metric_sql.zip \
--dt_start 1 \
--dt_end 2 \
--engine big_query \
--keyfile /home/andy/thefuck/airasia-opdatalake-stg-terraform.json \
--configs ../config.yml \
--gcs_temp_bucket airasia-metrics-data-stg