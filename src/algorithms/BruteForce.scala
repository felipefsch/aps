package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.xml.XML
import utils._

/**
 * Brute Force algorithm:
 * -generates all combination of pairs using Spark's cartesian product
 * -avoid duplicates by ordering the pairs ID
 */
object BruteForce {
 
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    
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
      val ranksArray = Load.spaceSeparated(input, sc, Args.partitions)
    
      // Denormalize threshold
      val threshold = Footrule.denormalizeThreshold(Args.k, normThreshold)      
  
      // Cartesian product
      val cartesianRanks = CartesianProduct.orderedWithoutSelf(ranksArray)
      
      val allDistances = cartesianRanks.map(x => Footrule.onLeftIdIndexedArray(x))
      
      //Filter with threshold, keep equal elements
      val similarRanks = allDistances.filter(x => x._2 <= threshold)

      if (storeCount)
        Store.rddToLocalAndCount(output, similarRanks)
      else
        Store.rddToLocalMachine(output, similarRanks)
      
    } finally {
      // Force stopping Spark Context before exiting the algorithm 
      sc.stop()
    }
  }
  
}