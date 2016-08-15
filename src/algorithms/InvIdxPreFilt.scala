package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._

object InvIdxPreFilt {
   
  def main(args: Array[String]): Unit = {
    Args.parse(args)
   
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
      val ranksArray =  Load.spaceSeparated(input, sc, Args.partitions)
    
      var threshold = Footrule.denormalizeThreshold(Args.k, normThreshold)      
      
      var prefixSize = Args.k - Footrule.getMinOverlap(Args.k, threshold)
      
      val invertedIndex = InvertedIndex.getInvertedIndex(ranksArray, prefixSize.toInt)
      
      val distinctCandidates = InvertedIndex.getCandidates(invertedIndex)
      
      val allDistances = distinctCandidates.map(x => Footrule.onLeftIdIndexedArray(x))
      
      // Move distinct() to previous lines to avoid unnecessary computation
      val similarRanks = allDistances.filter(x => x._2 <= threshold).distinct() // TODO: check if distinct() necessary!!!
      
      if (storeCount)
        Store.rddToLocalAndCount(output, similarRanks)
      else
        Store.rddToLocalMachine(output, similarRanks)
      
    } finally {
      sc.stop()
    }
  }
}