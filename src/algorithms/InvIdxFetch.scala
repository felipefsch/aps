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
           
      if (Args.PREGROUP)
        ranksArray = Duplicates.findDuplicates(ranksArray, output)    
      
      val invertedIndex = InvertedIndex.getInvertedIndexIDs(ranksArray, Args.k)      
      val flatInvIdx = invertedIndex.flatMap(x => x._2)
      
      val distinctCandidates = InvertedIndex.getCandidatesIDs(invertedIndex)  
  
      // Join on rankId1 and transform output to (rankId2, (rankId1, (elements1))
      val firstJoin = ranksArray.join(distinctCandidates).map(x => (x._2._2, (x._1, x._2._1)))
      // Join on rankId2 and transform output to ((rankId1, elements1), (rankId2, elements2)) 
      val secondJoin = ranksArray.join(firstJoin).map(x => (x._2._2, (x._1, x._2._1)))
  
      val allDistances = secondJoin.map(x => Footrule.onLeftIdIndexedArray(x))
      
      var similarRanks = allDistances.filter(x => x._2 <= Args.threshold)
            
      if (Args.PREGROUP) {
        var duplicates = Duplicates.getDuplicates(ranksArray)
        var expandedDuplicates = Duplicates.expandDuplicates(duplicates)
        similarRanks = similarRanks.union(expandedDuplicates)
      }        
      
      Store.rdd(output, ranksArray, Args.COUNT, Args.STORERESULTS, similarRanks, Args.EXPANDRESULTS)
      
    } finally {
      sc.stop()
    }
  }
}