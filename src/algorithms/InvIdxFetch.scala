package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._
import benchmark.Profiling

object InvIdxFetch {
  
  def main(args: Array[String]): Unit = {
    val sc = Config.getSparkContext(args)
  
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetch"

    try {
      // Load also sets ranking size k
      var ranksArray =  Load.loadData(input, sc, Args.partitions) 
           
      if (Args.GROUPDUPLICATES)
        ranksArray = Duplicates.groupDuplicates(ranksArray)    
      
      val invertedIndex = InvertedIndex.getInvertedIndexIDs(ranksArray, Args.k)      
      val flatInvIdx = invertedIndex.flatMap(x => x._2)
      
      val distinctCandidates = InvertedIndex.getCandidatesIDs(invertedIndex)  
  
      // Join on rankId1 and transform output to (rankId2, (rankId1, (elements1))
      val firstJoin = ranksArray.join(distinctCandidates).map(x => (x._2._2, (x._1, x._2._1)))
      // Join on rankId2 and transform output to ((rankId1, elements1), (rankId2, elements2)) 
      val secondJoin = ranksArray.join(firstJoin).map(x => (x._2._2, (x._1, x._2._1)))
  
      val allDistances = secondJoin.map(x => Footrule.onLeftIdIndexedArray(x))
      
      var similarRanks = allDistances.filter(x => x._2 <= Args.threshold)
            
      if (Args.GROUPDUPLICATES) {
        var duplicates = Duplicates.getDuplicates(ranksArray)
        var rddUnion = similarRanks.union(duplicates)
        similarRanks = Duplicates.expandDuplicates(rddUnion)
      }

      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)
      
    } finally {
      sc.stop()
    }
  }
}