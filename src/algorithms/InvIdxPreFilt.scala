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
   
    var begin, end = 0.toLong
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxPreFilt"

    try {  
      // Load also sets ranking size k
      begin = System.nanoTime()
      var ranksArray =  Load.loadData(input, sc, Args.partitions)
      end = System.nanoTime()
      Profiling.stageTime("load data", begin, end)        
      
      if (Args.PREGROUP)
        ranksArray = PreProcessing.groupDuplicatesAndStore(ranksArray, output)          

      var prefixSize = Footrule.getPrefixSize(Args.k, Args.threshold)
      
      begin = System.nanoTime() 
      val invertedIndex = InvertedIndex.getInvertedIndex(ranksArray, prefixSize.toInt)
      end = System.nanoTime()
      Profiling.stageTime("create inverted index", begin, end)        
      
      begin = System.nanoTime() 
      val distinctCandidates = InvertedIndex.getCandidates(invertedIndex)
      end = System.nanoTime()
      Profiling.stageTime("get candidates", begin, end)        
      
      begin = System.nanoTime() 
      val allDistances = distinctCandidates.map(x => Footrule.onLeftIdIndexedArray(x))
      end = System.nanoTime()
      Profiling.stageTime("compute distances", begin, end)        
      
      // Move distinct() to previous lines to avoid unnecessary computation
      begin = System.nanoTime() 
      var similarRanks = allDistances.filter(x => x._2 <= Args.threshold).distinct()
      end = System.nanoTime()
      Profiling.stageTime("filter on threshold", begin, end)  

      if (Args.PREGROUP) {
        var duplicates = PreProcessing.getDuplicate(ranksArray)
        var expandedDuplicates = PreProcessing.expandDuplicates(duplicates)
        similarRanks = similarRanks.union(expandedDuplicates)
      }      
      
      // Saving output locally on each node
      begin = System.nanoTime()        
      Store.rdd(output, ranksArray, Args.COUNT, Args.STORERESULTS, similarRanks, Args.EXPANDRESULTS)
      end = System.nanoTime()
      Profiling.stageTime("store results", begin, end)     
      
    } finally {
      sc.stop()
    }
  }
}