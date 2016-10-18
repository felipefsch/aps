package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j._


import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._

object InvIdxPreFetch {
  
  /**
   * Input:
   * -in: (ID, [Elements])*
   * -threshold: Denormalized Footrule distance
   * 
   * Output:
   * -((ID1, ID2), Distance)*
   * 
   * Main logic of approach. 
   */
  def run(in: RDD[(String, Array[String])], threshold: Long, k: Int)
  : RDD[((String, String), Long)] = {
    var prefixSize = Footrule.getPrefixSize(k, threshold)
            
    val invertedIndex = InvertedIndex.getInvertedIndexIDs(in, prefixSize.toInt)      
    
    val distinctCandidates = InvertedIndex.getCandidatesIDs(invertedIndex)      

    // Join on rankId1 and transform output to (rankId2, (rankId1, (elements1))
    val firstJoin = in.join(distinctCandidates).map(x => (x._2._2, (x._1, x._2._1)))      
    // Join on rankId2 and transform output to ((rankId1, elements1), (rankId2, elements2)) 
    val secondJoin = in.join(firstJoin).map(x => (x._2._2, (x._1, x._2._1)))   
    
    val allDistances = secondJoin.map(x => Footrule.onLeftIdIndexedArray(x))      
    
    // Move distinct() to previous lines to avoid unnecessary computation
    var similarRanks = allDistances.filter(x => x._2 <= threshold).distinct()
    
    return similarRanks
  }
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)

    // Variables not modifiable. Important when running on a cluster
    // so that all nodes have the correct values
    val output = Args.output + "InvIdxFetchPreFilt"
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
      var ranksArray = Load.loadData(input, sc, partitions, k, n)   

      var duplicates : org.apache.spark.rdd.RDD[((String, String), Long)] = sc.emptyRDD      
      if (GROUPDUPLICATES) {
        ranksArray = Duplicates.groupDuplicates(ranksArray)
        duplicates = Duplicates.getDuplicates(ranksArray)        
      }          
           
      var similarRanks = run(ranksArray, threshold, k)
      
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
      Config.closeSparkContext(sc)
    }
  }
}