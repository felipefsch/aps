package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.xml.XML
import utils._
import org.apache.log4j._


/**
 * Brute Force algorithm:
 * -generates all combination of pairs using Spark's cartesian product
 * -avoid duplicates by ordering the pairs ID
 */
object BruteForce {
 
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    
    // Variables not modifiable. Important when running on a cluster
    // so that all nodes have the correct values    
    val output = Args.output + "BruteForce"
    val masterIp = Args.masterIp
    val threshold = Args.threshold
    val normThreshold = Args.normThreshold
    val input = Args.input
    val k = Args.k
    val n = Args.n
    val minOverlap = Args.minOverlap
    val hdfsUri = Args.hdfsUri
    val partitions = Args.partitions
    val COUNT = Args.COUNT
    val DEBUG = Args.DEBUG
    val STORERESULTS = Args.STORERESULTS      
    val GROUPDUPLICATES = Args.GROUPDUPLICATES    
    
    val sc = Config.getSparkContext(masterIp)
    
    try {
      // Load also sets ranking size k  
      var ranksArray = Load.file(input, sc, partitions, k, n)
      
      var duplicates : org.apache.spark.rdd.RDD[((String, String), Long)] = sc.emptyRDD      
      if (GROUPDUPLICATES) {
        ranksArray = Duplicates.groupDuplicates(ranksArray)
        duplicates = Duplicates.getDuplicates(ranksArray)        
      }  
      
      // Cartesian product
      val cartesianRanks = CartesianProduct.orderedWithoutSelf(ranksArray)
      
      val allDistances = cartesianRanks.map(x => Footrule.onLeftIdIndexedArray(x))
      
      //Filter with threshold, keep equal elements
      var similarRanks = allDistances.filter(x => x._2 <= threshold)

      if (GROUPDUPLICATES) {
        var rddUnion = similarRanks.union(duplicates)
        similarRanks = Duplicates.expandDuplicates(rddUnion)
      }

      Store.rdd(output, similarRanks, COUNT, STORERESULTS, hdfsUri)
    } catch {
      case e:
        Exception => val log = LogManager.getRootLogger
        log.error(e.toString())      
    } finally {
      // Force stopping Spark Context before exiting the algorithm 
      Config.closeSparkContext(sc)
    }
  }
  
}