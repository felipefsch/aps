Master thesis code. When importing code to Eclipse, keep in mind you should also add ALL jar files inside sparkFolder/lib folder to eclipse library (should be provided inside spark 1.6.2 folder). Also HDFS files should be provided (under hadoopFolder/share/hadoop/hdfs/).

Code works with:
* Spark 1.6.2
* Scala 2.10.8
* Java 1.7.0_95

To run it, add the following arguments Benchmark.scala

```
--masterIp local --input input/realDataSample.txt --writeAll false --nExecs 1 --output output/ --benchmarkOutput benchmark/test.txt --count true --threshold 0.3 --k 10 --n 0 --groupduplicates false --bruteforce true --elementsplit false --invidx false  --invidxpre false --invidxfetch false --invidxprefetch false
```

Possible errors:

WARN NettyRpcEndpointRef: Error sending message [message = Heartbeat(driver,[Lscala.Tuple2;@26a290f,BlockManagerId(driver, localhost, 33036))] in 1 attempts
org.apache.spark.rpc.RpcTimeoutException: Futures timed out after [10 seconds]. This timeout is controlled by spark.executor.heartbeatInterval

This can be solved by increasing spark.executor.heartbeatInterval value (e.g, 20s). [https://issues.apache.org/jira/browse/SPARK-14140]

