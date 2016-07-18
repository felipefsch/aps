package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._

object InvIdxFetch {

  /**
   * Given rank ID and its elements, create tuples as (rankId, element(i))
   * for all elements in the rank
   */
  def arrayToIdElement(in: (Long, Array[Long]))
  : scala.collection.immutable.IndexedSeq[(Long, Long)] = {
    var id = in._1
    var elements = in._2
    
    for (i <- 0 until elements.length) yield {
      (id, elements(i))
    }
  }
  
  /**
   * Create inverted index for all distinct rank elements
   */
  def getInvertedIndex(ranksArray: RDD[(Long, Array[Long])])
  : RDD[(Long, Iterable[(Long,Long)])] = {
    // Create one tuple for each element (id, element)
    val tuples = ranksArray.flatMap(x => arrayToIdElement(x))
  
    // Inverted index as: Array[element, (rankId, element)*]
    // TODO: avoid element duplicated into each tuple on Iterable
    val invertedIndex = tuples.groupBy(tup => tup._2)    
    
    return invertedIndex
  }
  
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
      val ranksArray =  Load.spaceSeparated(input, sc)
      
      val invertedIndex = getInvertedIndex(ranksArray)
      
      val flatInvIdx = invertedIndex.flatMap(x => x._2)
      
      val distinctCandidates = flatInvIdx.cartesian(flatInvIdx).filter(x => (x._1._2 == x._2._2) && (x._1._1 < x._2._1)).map(x => (x._1._1, x._2._1)).distinct()      
  
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