package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._
import benchmark.Profiling

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
  def run(in: RDD[(String, Array[String])], threshold: Long)
  : RDD[((String, String), Long)] = {
    var prefixSize = Footrule.getPrefixSize(Args.k, threshold)
          
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
    val sc = Config.getSparkContext(args)
 
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetchPreFilt"

    try {  
      // Load also sets ranking size k
      var ranksArray =  Load.loadData(input, sc, Args.partitions)   

      if (Args.PREGROUP)
        ranksArray = Duplicates.findDuplicates(ranksArray, output)          
           
      var similarRanks = run(ranksArray, Args.threshold)
      
      if (Args.PREGROUP) {
        var duplicates = Duplicates.getDuplicates(ranksArray)
        var rddUnion = similarRanks.union(duplicates)
        if (false)
          similarRanks = Duplicates.expandDuplicates(rddUnion)
        else
          similarRanks = rddUnion
      }

      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)

    } finally {
      sc.stop()
    }
  }
}