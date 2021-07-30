SUBMIT=.venv/lib/python3.7/site-packages/pyspark/bin/spark-submit
sh $SUBMIT  main.py --metric_db "resources/metrics.zip" --sql_archive "resources/metric_sql.zip" --pymod_archive "resources/metric_pymod.zip" --configs "resources/config.yml" --engine "big_query" --dt_start "2021-07-28T00:00:00+00:00" --dt_end "2021-07-29T00:00:00+00:00" --gcs_temp_bucket "bucket" --keyfile "key.json"
