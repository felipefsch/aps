# First add the project jar to the same folder as this script. 
# You should have folders "inputs", "outputs" and "benchmarking"
# one level before than this script in the folders tree.
#
# Notice that the first one should contain the input data, in this case, nyt.msn.mi.left
#
# Memory usage and so on should also be tuned on this script

SPARK_BIN=/opt/spark-2.0/bin

INPUT=${PWD}/../inputs/nyt.msn.mi.left
OUTPUT=${PWD}/../outputs/
K=10
N=0
THRESHOLD=0.05
BENCHMARK_OUTPUT=${PWD}/../benchmarking/100kreal_benchmark.txt

cd $SPARK_BIN

sudo ./spark-submit --executor-memory=100g --driver-memory=100g --class benchmark.Benchmark ~/code/aps2.0.jar --input $INPUT --writeAll false --nExecs 1 --output $OUTPUT --benchmarkOutput $BENCHMARK_OUTPUT --pregroup false --k $K --n $N --threshold $THRESHOLD --bruteforce false --invidx true --invidxpre true --invidxfetch true --invidxprefetch true --elementsplit true

cd -