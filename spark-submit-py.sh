spark-submit --master yarn --deploy-mode cluster --num-executors 6 --executor-cores 2 --executor-memory 2g --driver-memory 2g --conf spark.testing.memory=2000000000 --conf spark.driver.maxResultSize=2g $1 $2 $3 $4 $5 $6 $7 $8 $9