#source ~/.bashrc
#MASTER=spark://dbis-expsrv3:7077

SPARK_BIN=/opt/spark-2.0/bin
APS_JAR=${PWD}/aps2.0.jar
MASTER=yarn

BENCHMARK_OUTPUT=${PWD}/../benchmarking/300ksize10_32_partitions_NOT_OPTIMIZED.txt
INPUT=hdfs:/inputs/500k.nyt.msn.mi.left.indexed.size10
OUTPUT=hdfs:/outputs/

K=10
N=300000
THRESHOLD=0.1
THRESHOLD_C=0.05
PARTITIONS=32

# It considers the driver as an executor as well!
NUM_EXECS=8
# Should be at most 10% smaller than maximum allowed by container
EXEC_MEM=11000M #11000M #24200M #50000M  #102298M
EXEC_CORES=1
DRIVER_MEM=2G

COUNT=false
DEBUG=false
GROUP_DUPLICATES=false

SLEEP_TIME=1m

cd $SPARK_BIN 

#### METRIC SPACE ###
#hadoop fs -rm -r /outputs/MetricSpace
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --metricspace true
#
#sleep $SLEEP_TIME
#
#### ELEMENT SPLIT ###
#hadoop fs -rm -r /outputs/ElementSplit
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --elementsplit true
#
#sleep $SLEEP_TIME
#
#### INV IDX FETCH PRE FILT ###
#hadoop fs -rm -r /outputs/InvIdxFetchPreFilt
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidxprefetch true
#
#sleep $SLEEP_TIME
#
### INV IDX FETCH PRE FILT C ###
hadoop fs -rm -r /outputs/InvIdxFetchPreFilt_c

./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidxprefetch_c true

sleep $SLEEP_TIME

#### INV IDX FETCH ###
#hadoop fs -rm -r /outputs/InvIdxFetch
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidxfetch true
#
#sleep $SLEEP_TIME
#
### ELEMENT SPLIT C ###
hadoop fs -rm -r /outputs/ElementSplit_c

./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --elementsplit_c true

sleep $SLEEP_TIME

### INV IDX PRE FILT ###
hadoop fs -rm -r /outputs/InvIdxPreFilt

./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidxpre true

#### INV IDX ###
#hadoop fs -rm -r /outputs/InvIdx
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidx true
#
#sleep $SLEEP_TIME
#
#### BRUTE FORCE ###
#hadoop fs -rm -r /outputs/BruteForce
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --bruteforce true
#
#sleep $SLEEP_TIME
#


#BENCHMARK_OUTPUT=${PWD}/../benchmarking/100ksize10_32_partitions_DUPLICATES.txt
#BENCHMARK_OUTPUT=/home/fschmidt/benchmarking/set_to_near_duplicate.txt
#N=100000
#THRESHOLD=0.2
#THRESHOLD_C=0.05
#
#### ELEMENT SPLIT ###
#hadoop fs -rm -r /outputs/ElementSplit
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --elementsplit true
#
#sleep $SLEEP_TIME
#
#### INV IDX FETCH PRE FILT ###
#hadoop fs -rm -r /outputs/InvIdxFetchPreFilt
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidxprefetch true
#
#sleep $SLEEP_TIME
#
#
##PARTITIONS=8
##NUM_EXECS=4
##EXEC_MEM=24200M #11000M #24200M #50000M  #102298M
##EXEC_CORES=2
#### INV IDX FETCH PRE FILT C ###
#hadoop fs -rm -r /outputs/InvIdxFetchPreFilt_c
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidxprefetch_c true
#
#sleep $SLEEP_TIME
#
### INV IDX FETCH ###
#hadoop fs -rm -r /outputs/InvIdxFetch
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --invidxfetch true
#
#sleep $SLEEP_TIME
#
#### ELEMENT SPLIT C ###
#hadoop fs -rm -r /outputs/ElementSplit_c
#
#./spark-submit --num-executors $NUM_EXECS --executor-cores $EXEC_CORES --executor-memory $EXEC_MEM --driver-memory $DRIVER_MEM --master $MASTER --deploy-mode cluster --class benchmark.Benchmark $APS_JAR --input $INPUT --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --groupduplicates $GROUP_DUPLICATES --k $K --n $N --threshold $THRESHOLD --threshold_c $THRESHOLD_C --count $COUNT --debug $DEBUG --partitions $PARTITIONS --elementsplit_c true
#
#sleep $SLEEP_TIME
#
cd -
