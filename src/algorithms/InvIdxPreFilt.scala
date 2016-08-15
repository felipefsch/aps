package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._
import benchmark.Benchmark

object InvIdxPreFilt {
   
  def main(args: Array[String]): Unit = {
    Args.parse(args)
   
    var begin, end = 0.toLong
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxPreFilt"
    var master = Args.masterIp    
    var storeCount = Args.COUNT    
    
    val conf = new SparkConf().setMaster(master)
              .setMaster(master)
              .setAppName("invertedIndexPrefixFiltFetchID")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.executor.cores", Args.cores)
              .set("spark.executor.instances", Args.executors)
    
    val sc = new SparkContext(conf)
    try {  
      // Load also sets ranking size k
      begin = System.nanoTime()
      val ranksArray =  Load.spaceSeparated(input, sc, Args.partitions)
      end = System.nanoTime()
      Benchmark.stageTime("load data", begin, end)        

      var prefixSize = Args.k - Footrule.getMinOverlap(Args.k, Args.threshold)
      
      begin = System.nanoTime() 
      val invertedIndex = InvertedIndex.getInvertedIndex(ranksArray, prefixSize.toInt)
      end = System.nanoTime()
      Benchmark.stageTime("create inverted index", begin, end)        
      
      begin = System.nanoTime() 
      val distinctCandidates = InvertedIndex.getCandidates(invertedIndex)
      end = System.nanoTime()
      Benchmark.stageTime("get candidates", begin, end)        
      
      begin = System.nanoTime() 
      val allDistances = distinctCandidates.map(x => Footrule.onLeftIdIndexedArray(x))
      end = System.nanoTime()
      Benchmark.stageTime("compute distances", begin, end)        
      
      // Move distinct() to previous lines to avoid unnecessary computation
      begin = System.nanoTime() 
      val similarRanks = allDistances.filter(x => x._2 <= Args.threshold).distinct()
      end = System.nanoTime()
      Benchmark.stageTime("filter on threshold", begin, end)        
      
      // Saving output locally on each node
      begin = System.nanoTime()        
      if (storeCount) {
        Store.rddToLocalAndCount(output, similarRanks)
      }
      else {
        Store.rddToLocalMachine(output, similarRanks)
      }
      end = System.nanoTime()
      Benchmark.stageTime("store results", begin, end)     
      
    } finally {
      sc.stop()
    }
  }
}