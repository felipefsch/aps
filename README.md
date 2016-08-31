Master thesis code. When importing code to Eclipse, keep in mind you should also add ALL jar files inside spark/jars folder to eclipse library (should be provided inside spark 2.0.0 folder).

Code works with:
*Spark 2.0.0
*Scala 2.11.6
*Java 1.7.0_95

To run it, add the following arguments Benchmark.scala

--input input/limitcase0.5.txt --writeAll false --nExecs 1 --output output/ --benchmarkOutput benchmark/test.txt --pregroup false --k 10 --n 1000 --threshold 0.1 --count true


NOTE (Arguments used on Lab Computer for testing):
--input /home/schmidt/Dropbox/Master/Thesis/svn/fschmidt/datasets/nyt.msn.mi.left --writeAll false --nExecs 1 --output output/ --benchmarkOutput benchmark/test.txt --pregroup false --k 10 --n 1000 --threshold 0.1 --count true --elementsplit true --bruteforce true --invidx true --invidxpre true --invidxfetch true --invidxprefetch true