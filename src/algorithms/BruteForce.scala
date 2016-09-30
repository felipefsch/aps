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
    val sc = Config.getSparkContext(args)
    
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "BruteForce"
    
    var storeCount = Args.COUNT
    
    try {
      // Load also sets ranking size k  
      var ranksArray = Load.loadData(input, sc, Args.partitions)
      
      if (Args.GROUPDUPLICATES)
        ranksArray = Duplicates.groupDuplicates(ranksArray)        
      
      // Cartesian product
      val cartesianRanks = CartesianProduct.orderedWithoutSelf(ranksArray)   
      
      val allDistances = cartesianRanks.map(x => Footrule.onLeftIdIndexedArray(x))     
      
      //Filter with threshold, keep equal elements
      var similarRanks = allDistances.filter(x => x._2 <= Args.threshold)
      
      if (Args.GROUPDUPLICATES) {
        var duplicates = Duplicates.getDuplicates(ranksArray)
        var rddUnion = similarRanks.union(duplicates)
        similarRanks = Duplicates.expandDuplicates(rddUnion)
      }

      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)
      
    } finally {
      // Force stopping Spark Context before exiting the algorithm 
      Config.closeSparkContext(sc)
    }
  }
  
}