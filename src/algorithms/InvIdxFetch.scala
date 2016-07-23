package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._

object InvIdxFetch {
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)

    val configXml = Args.configXml
    
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetch"
    var master = ((configXml \\ "config") \\ "masterIp").text
    var k = Args.k
    var storeCount = Args.COUNT    
    
    val threshold = Footrule.denormalizeThreshold(k, normThreshold)
    
    val conf = new SparkConf().setMaster(master).setAppName("InvertedIndexId")
    
    val sc = new SparkContext(conf)
    try {
      // Partition ranks
      val ranksArray =  Load.spaceSeparated(input, sc, Args.nodes)
      
      val invertedIndex = InvertedIndex.getInvertedIndexIDs(ranksArray, k)
      
      val flatInvIdx = invertedIndex.flatMap(x => x._2)
      
      val distinctCandidates = InvertedIndex.getCandidatesIDs(invertedIndex)      
  
      // Join on rankId1 and transform output to (rankId2, (rankId1, (elements1))
      val firstJoin = ranksArray.join(distinctCandidates).map(x => (x._2._2, (x._1, x._2._1)))
      // Join on rankId2 and transform output to ((rankId1, elements1), (rankId2, elements2)) 
      val secondJoin = ranksArray.join(firstJoin).map(x => (x._2._2, (x._1, x._2._1)))
  
      val allDistances = secondJoin.map(x => Footrule.onLeftIdIndexedArray(x))
      
      val similarRanks = allDistances.filter(x => x._2 <= threshold)
      
      if (storeCount)
        Store.rddToLocalAndCount(output, similarRanks)
      else
        Store.rddToLocalMachine(output, similarRanks)
    } finally {
      sc.stop()
    }
  }
}