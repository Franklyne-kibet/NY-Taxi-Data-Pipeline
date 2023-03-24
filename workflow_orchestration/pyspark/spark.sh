URL="local[*]"
spark-submit \
    --master="${URL}" \
    --driver-memory 2g \
    pyspark/spark_job.py
    