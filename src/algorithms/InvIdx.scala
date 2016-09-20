package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._
import benchmark.Profiling

object InvIdx {
  
  def main(args: Array[String]): Unit = {
    val sc = Config.getSparkContext(args)
   
    var begin, end = 0.toLong
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdx" 
    
    try {  
      // Load also sets ranking size k
      var ranksArray = Load.loadData(input, sc, Args.partitions) 
      
      if (Args.GROUPDUPLICATES)
        ranksArray = Duplicates.groupDuplicates(ranksArray)      
       
      val invertedIndex = InvertedIndex.getInvertedIndex(ranksArray, Args.k)
      
      val distinctCandidates = InvertedIndex.getCandidates(invertedIndex)  

      val allDistances = distinctCandidates.map(x => Footrule.onLeftIdIndexedArray(x))
      
      // Move distinct() to previous lines to avoid unnecessary computation
      var similarRanks = allDistances.filter(x => x._2 <= Args.threshold).distinct()
            
      if (Args.GROUPDUPLICATES) {
        var duplicates = Duplicates.getDuplicates(ranksArray)
        var rddUnion = similarRanks.union(duplicates)
        if (Args.EXPANDDUPLICATES)
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