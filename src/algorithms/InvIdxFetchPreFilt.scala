package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._
import benchmark.Profiling

object InvIdxFetchPreFilt {
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
 
    var begin, end = 0.toLong
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetchPreFilt"
    var master = Args.masterIp
    
    val conf = new SparkConf()
              .setMaster(master)
              .setAppName("invertedIndexPrefixFiltering")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.executor.cores", Args.cores)
              .set("spark.executor.instances", Args.executors)
    
    val sc = new SparkContext(conf)
    try {  
      // Load also sets ranking size k
      begin = System.nanoTime()
      var ranksArray =  Load.loadData(input, sc, Args.partitions)   
      end = System.nanoTime()
      Profiling.stageTime("load data", begin, end)      

      if (Args.PREGROUP)
        ranksArray = PreProcessing.groupDuplicatesAndStore(ranksArray, output)          
           
      var prefixSize = Args.k - Footrule.getMinOverlap(Args.k, Args.threshold) 
            
      end = System.nanoTime()
      val invertedIndex = InvertedIndex.getInvertedIndexIDs(ranksArray, prefixSize.toInt)
      end = System.nanoTime()
      Profiling.stageTime("store results", begin, end)      
      
      begin = System.nanoTime()
      val distinctCandidates = InvertedIndex.getCandidatesIDs(invertedIndex)
      end = System.nanoTime()
      Profiling.stageTime("get candidates", begin, end)      

      // Join on rankId1 and transform output to (rankId2, (rankId1, (elements1))
      begin = System.nanoTime()
      val firstJoin = ranksArray.join(distinctCandidates).map(x => (x._2._2, (x._1, x._2._1)))      
      // Join on rankId2 and transform output to ((rankId1, elements1), (rankId2, elements2)) 
      val secondJoin = ranksArray.join(firstJoin).map(x => (x._2._2, (x._1, x._2._1)))   
      end = System.nanoTime()
      Profiling.stageTime("fetch raking from IDs", begin, end)      
      
      begin = System.nanoTime()
      val allDistances = secondJoin.map(x => Footrule.onLeftIdIndexedArray(x))
      end = System.nanoTime()
      Profiling.stageTime("compute distances", begin, end)      
      
      // Move distinct() to previous lines to avoid unnecessary computation
      begin = System.nanoTime()
      val similarRanks = allDistances.filter(x => x._2 <= Args.threshold).distinct()
      end = System.nanoTime()
      Profiling.stageTime("filter on threshold", begin, end)      
      
      // Saving output locally on each node
      begin = System.nanoTime()        
      Store.storeRdd(output, similarRanks, Args.COUNT)
      end = System.nanoTime()
      Profiling.stageTime("store results", begin, end)

    } finally {
      sc.stop()
    }
  }
}