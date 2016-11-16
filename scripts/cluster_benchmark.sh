BENCHMARK_FILE=/home/schmidt/svn/fschmidt/Thesis/Benchmarks/300k_element_split

echo "" >> $BENCHMARK_FILE
date >> $BENCHMARK_FILE

START_TIME=$(date +%s%3N)

SPARK_DIR=/opt/spark-1.6.1-bin-hadoop2.6/bin

N=200000

cd $SPARK_DIR

./spark-submit --driver-memory 2G --num-executors 26 --executor-memory 4G --executor-cores 1 --master yarn-cluster --conf spark.yarn.jar=hdfs://dbis-expsrv4.informatik.uni-kl.de:8020/user/schmidt/jar/spark-assembly-1.6.1-hadoop2.6.0.jar --class algorithms.ElementSplit hdfs://dbis-expsrv4.informatik.uni-kl.de:8020/user/schmidt/jar/aps1.6.repartition.jar --input hdfs://dbis-expsrv4.informatik.uni-kl.de:8020/user/schmidt/inputs/500k.nyt.msn.mi.left.indexed.size10 --output hdfs://dbis-expsrv4.informatik.uni-kl.de:8020/user/schmidt/outputs/ --groupduplicates false --k 10 --n $N --threshold 0.1 --threshold_c 0.05 --count false --debug false --partitions 26 --hdfsUri hdfs://dbis-expsrv4.informatik.uni-kl.de:8020

cd -

END_TIME=$(date +%s%3N)

ELAPSED_TIME=$((END_TIME  - START_TIME))

echo "Element Split: " $ELAPSED_TIME " ns" >> $BENCHMARK_FILE
echo "" >> $BENCHMARK_FILE
