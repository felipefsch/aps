package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._

object InvIdxFetchPreFilt {
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
 
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetchPreFilt"
    var master = Args.masterIp
    var storeCount = Args.COUNT
    
    val conf = new SparkConf()
              .setMaster(master)
              .setAppName("invertedIndexPrefixFiltering")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.executor.cores", Args.cores)
              .set("spark.executor.instances", Args.executors)
    
    val sc = new SparkContext(conf)
    try {  
      // Load also sets ranking size k
      val ranksArray =  Load.spaceSeparated(input, sc, Args.partitions)
    
      val threshold = Footrule.denormalizeThreshold(Args.k, normThreshold)      
           
      var prefixSize = Args.k - Footrule.getMinOverlap(Args.k, threshold) 
      
      val invertedIndex = InvertedIndex.getInvertedIndexIDs(ranksArray, prefixSize.toInt)
      
      val distinctCandidates = InvertedIndex.getCandidatesIDs(invertedIndex)  

      // Join on rankId1 and transform output to (rankId2, (rankId1, (elements1))
      val firstJoin = ranksArray.join(distinctCandidates).map(x => (x._2._2, (x._1, x._2._1)))
      
      // Join on rankId2 and transform output to ((rankId1, elements1), (rankId2, elements2)) 
      val secondJoin = ranksArray.join(firstJoin).map(x => (x._2._2, (x._1, x._2._1)))      
      
      val allDistances = secondJoin.map(x => Footrule.onLeftIdIndexedArray(x))
      
      // Move distinct() to previous lines to avoid unnecessary computation
      val similarRanks = allDistances.filter(x => x._2 <= threshold).distinct()
      
      if (storeCount)
        Store.rddToLocalAndCount(output, similarRanks)
      else
        Store.rddToLocalMachine(output, similarRanks)
      
    } finally {
      sc.stop()
    }
  }
}