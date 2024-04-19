# python3 11_spark_cluster_sql.py \
#   --input_green=../../../data/pg/green/2020/*/ \
#   --input_yellow=../../../data/pg/yellow/2020/*/ \
#   --output=../../../data/report/report-2020

URL="spark://DE-C02Y61A5JGH6:7077"

spark-submit \
  --master="${URL}" \
  11_spark_cluster_sql.py \
    --input_green=../../../data/pg/green/2021/*/ \
    --input_yellow=../../../data/pg/yellow/2021/*/ \
    --output=../../../data/report/report-2021