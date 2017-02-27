# About

Master thesis code. When importing code to Eclipse, keep in mind you should also add ALL jar files inside sparkFolder/lib folder to eclipse library (should be provided inside Spark's folder). Also HDFS files should be provided (under hadoopFolder/share/hadoop/hdfs/).

# Environment

Code works with:
* Spark 2.0.0
* Scala 2.11.8
* Hadoop 2.7.3
* Java 1.8.0_91

Also possible to use versions:
* Spark 1.6.2
* Scala 2.10.8
* Hadoop 2.6
* Java 1.7.0_95

Just need to import the desired version of libraries.

# Eclipse

1. download [Scala IDE](http://scala-ide.org/download/sdk.html)
2. clone project into your workspace
3. create new scala project, with source files from cloned repo
4. set correct java and spark versions
5. import spark and hadoop jars

## Running from Eclipse


To run it, add the following arguments Benchmark.scala

```
--masterIp local --input input/realDataSample.txt --writeAll false --nExecs 1 --output output/ --benchmarkOutput benchmark/test.txt --count true --threshold 0.3 --k 10 --n 0 --groupduplicates false --bruteforce true --elementsplit false --invidx false  --invidxpre false --invidxfetch false --invidxprefetch false
```

# Spark Submit

Configure spark (preferably to run with Yarn) and xport jar file from project. Under spark/bin run the following line with the desired parameters:


```
./spark-submit --num-executors 8 --executor-cores 1 --executor-memory 11G --driver-memory 2G --master yarn --deploy-mode cluster --class benchmark.Benchmark /PATH\_TO\_JAR.jar --input hdfs:/FILE/PATH --nExecs 1 --output hdfs:/FOLDER/PATH/ --benchmarkOutput /PATH/TO/FOLDER --groupduplicates false --k 10 --n 1000 --threshold 0.1 --threshold_c 0.05 --count true --debug false --partitions 32 --invidxprefetch true
```

When storing into HDFS, don't forget to set hdfsUri so that the code properly have access to it.

```
--hdfsUri hdfs://server.uri:port/
```

"run_benchmark.sh" under scripts folder can be used as example.

# Generating Synthetic Data Set

It is possible to generate synthetic data either with Eclipse or submitting to Spark, as the following example:

```
./spark-submit --class benchmark.Benchmark /PATH\_TO\_JAR.jar --n 100000 --k 10 --nElements 50000 --benchmark false --datasetOutput /PATH\_TO\_OUTPUT.txt --createData true --nPools 10 --selectivity 0.05 --threshold 0.1
```

"create_data.sh" under scripts folder can be used as example.

# Parameters

The following parameters can be tunned to execute the code.

usage: class [options] ...
classes:
   algorithms.Init
   algorithms.BruteForce
   algorithms.ElementSplit
   algorithms.ElementSplitNearDuplicates
   algorithms.InvIdx
   algorithms.InvIdxPre
   algorithms.InvIdxFetch
   algorithms.InvIdxPreFetch
   algorithms.InvIdxPreFetchNearDuplicates
   benchmark.Benchmark
   benchmark.SyntheticDataSet

general options: 
   --k                    N    : ranking size
   --n                    N    : number of rankings
   --threshold            N.M  : normalized similarity threshold
   --threshold_c          N.M  : similarity threshold for near duplicates
   --groupduplicates      BOOL : group duplicates before checking for similars
   --duplicatesInput      PATH : folder with part-xxxxx files with similar rankings
   --input                PATH : input dataset path
   --output               PATH : result output path
   --storeresults         BOOL : store final results 
   --count                BOOL : count number of result pairs
   --debug                BOOL : debug mode
   --partitions           N    : number of partitions for repartitioning
   --executors            N    : number of executors on local machine
   --cores                N    : number of cores per executor
   --dynamicAlloc         BOOL : dynamically allocate executors
   --masterIp             IP   : master node IP
   --hdfsUri              URI  : URI of hdfs file system

benchmark specific:
   --nExecs               N    : number of executions of each algorithm
   --benchmarkOutput      PATH : benchmarking results output path
   --writeAll             BOOL : write execution time for each execution
   --createData           BOOL : create synthetic dataset
   --init                 BOOL : run Spark context initialization
   --bruteforce           BOOL : run brute force
   --elementsplit         BOOL : run elementsplit
   --elementsplit_c       BOOL : run elementsplit with near duplicates
   --invidx               BOOL : run inverted index
   --invidxpre            BOOL : run inverted index prefix filtering
   --invidxfetch          BOOL : run inverted index fetching IDs
   --invidxprefetch       BOOL : run inverted index prefix filtering fetch ID
   --invidxprefetch_c     BOOL : prefix filtering fetch ID with near duplicates
   --metricspace          BOOL : run search using metric space
   --benchmark            BOOL : run benchmarking (false dont run any approach)

synthetic dataset specific:
   --createData           BOOL : create synthetic dataset (overwrite existing)
   --datasetOutput        PATH : dataset output path (for synthetic data creation)
   --selectivity          N.M  : selectivity percentage
   --poolIntersection     N.M  : intersection percentage
   --nPools               N    : number of pools for intersecting rankings
   --nElements            N    : number of distinct elements
   
metric space (not finished):
   --medoidsmultiplier    INT  : how many medoids per partition
   
deprecated or limited support flags:
   --config               PATH : path to XML configuration file (LIMITED)
   --profiling            BOOL : profiling mode (DEPRECATED) 

# Troubleshooting

Possible errors:

WARN NettyRpcEndpointRef: Error sending message [message = Heartbeat(driver,[Lscala.Tuple2;@26a290f,BlockManagerId(driver, localhost, 33036))] in 1 attempts
org.apache.spark.rpc.RpcTimeoutException: Futures timed out after [10 seconds]. This timeout is controlled by spark.executor.heartbeatInterval

This can be solved by increasing spark.executor.heartbeatInterval value (e.g, 20s). [https://issues.apache.org/jira/browse/SPARK-14140]

The reason for such error is that the executors spend too much time with gargabe collection, so a better solution is either to run in a machine with more memory or reduce the input data.