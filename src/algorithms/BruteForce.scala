package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.xml.XML
import utils._
import benchmark.Profiling

/**
 * Brute Force algorithm:
 * -generates all combination of pairs using Spark's cartesian product
 * -avoid duplicates by ordering the pairs ID
 */
object BruteForce {
 
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    
    var begin, end = 0.toLong
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "BruteForce"
    
    var master = Args.masterIp
    var storeCount = Args.COUNT
    
    val conf = new SparkConf()
              .setMaster(master)
              .setAppName("bruteForce")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.executor.cores", Args.cores)
              .set("spark.executor.instances", Args.executors)
    
    val sc = new SparkContext(conf)
    
    try {
      // Load also sets ranking size k  
      begin = System.nanoTime()
      var ranksArray = Load.loadData(input, sc, Args.partitions)
      end = System.nanoTime()
      Profiling.stageTime("load data", begin, end)         
      
      if (Args.PREGROUP)
        ranksArray = PreProcessing.groupDuplicatesAndStore(ranksArray, output)        
      
      // Cartesian product
      begin = System.nanoTime()
      val cartesianRanks = CartesianProduct.orderedWithoutSelf(ranksArray)
      end = System.nanoTime()
      Profiling.stageTime("cartesian product", begin, end)      
      
      begin = System.nanoTime()
      val allDistances = cartesianRanks.map(x => Footrule.onLeftIdIndexedArray(x))
      end = System.nanoTime()
      Profiling.stageTime("compute distances", begin, end)      
      
      //Filter with threshold, keep equal elements
      begin = System.nanoTime()
      var similarRanks = allDistances.filter(x => x._2 <= Args.threshold)
      end = System.nanoTime()
      Profiling.stageTime("filter on threshold", begin, end)
      
      if (Args.PREGROUP) {
        var duplicates = PreProcessing.getDuplicate(ranksArray)
        similarRanks = similarRanks.union(duplicates)
      }

      begin = System.nanoTime()
      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)
      end = System.nanoTime()
      Profiling.stageTime("store results", begin, end)
      
    } finally {
      // Force stopping Spark Context before exiting the algorithm 
      sc.stop()
    }
  }
  
}