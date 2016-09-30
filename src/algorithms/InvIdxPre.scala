package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._
import benchmark.Profiling

object InvIdxPreFilt {
   
  def main(args: Array[String]): Unit = {
    val sc = Config.getSparkContext(args)
   
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxPreFilt"

    try {  
      // Load also sets ranking size k
      var ranksArray =  Load.loadData(input, sc, Args.partitions)        
      
      if (Args.GROUPDUPLICATES)
        ranksArray = Duplicates.groupDuplicates(ranksArray)          

      var prefixSize = Footrule.getPrefixSize(Args.k, Args.threshold)
      
      val invertedIndex = InvertedIndex.getInvertedIndex(ranksArray, prefixSize.toInt)        
      
      val distinctCandidates = InvertedIndex.getCandidates(invertedIndex)        
      
      val allDistances = distinctCandidates.map(x => Footrule.onLeftIdIndexedArray(x))        
      
      // Move distinct() to previous lines to avoid unnecessary computation
      var similarRanks = allDistances.filter(x => x._2 <= Args.threshold).distinct()

      if (Args.GROUPDUPLICATES) {
        var duplicates = Duplicates.getDuplicates(ranksArray)
        var rddUnion = similarRanks.union(duplicates)
        similarRanks = Duplicates.expandDuplicates(rddUnion)
      }

      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)
      
    } finally {
      Config.closeSparkContext(sc)
    }
  }
}