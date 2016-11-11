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
./spark-submit --num-executors 8 --executor-cores 1 --executor-memory 11G --master yarn --deploy-mode cluster --class benchmark.Benchmark /PATH\_TO\_JAR.jar --input INPUT\_FILE --writeAll false --nExecs 1 --output OUTPUT\_FILE --benchmarkOutput BENCHMARK\_LOCAL\_OUTPUT\_FILE --groupduplicates false --k K --n N --threshold THRESHOLD --threshold\_c THRESHOLD\_C --bruteforce false --invidx true --invidxpre true --invidxfetch true --invidxprefetch true --invidxprefetch_c true --elementsplit false --storeresults true --count true --expandduplicates false --groupnearduplicates false --debug false --partitions 32
```

When storing into HDFS, don't forget to set hdfsUri so that the code properly have access to it.

```
--hdfsUri hdfs://server.uri:port/
```

# Generating Synthetic Data Set

It is possible to generate synthetic data either with Eclipse or submitting to Spark, as the following example:

```
./spark-submit --class benchmark.Benchmark /PATH\_TO\_JAR.jar --n 100000 --k 10 --nElements 50000 --benchmark false --datasetOutput /PATH\_TO\_OUTPUT.txt --createData true --nPools 10 --selectivity 0.05 --threshold 0.1
```

# Troubleshooting

Possible errors:

WARN NettyRpcEndpointRef: Error sending message [message = Heartbeat(driver,[Lscala.Tuple2;@26a290f,BlockManagerId(driver, localhost, 33036))] in 1 attempts
org.apache.spark.rpc.RpcTimeoutException: Futures timed out after [10 seconds]. This timeout is controlled by spark.executor.heartbeatInterval

This can be solved by increasing spark.executor.heartbeatInterval value (e.g, 20s). [https://issues.apache.org/jira/browse/SPARK-14140]

The reason for such error is that the executors spend too much time with gargabe collection, so a better solution is either to run in a machine with more memory or reduce the input data.

