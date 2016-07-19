package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.xml.XML

import utils._

object InvIdx {

  /**
   * Given rank ID and its elements, create tuples as (rank, element(i))
   * for all elements in the rank
   */
  def arrayToIdElement(in: (Long, Array[Long]))
  : scala.collection.immutable.IndexedSeq[(Long, (Long, Array[Long]))] = {
    var id = in._1
    var rank = in._2
    var elements = in._2
    
    for (i <- 0 until elements.length) yield {
      (elements(i), (id, rank))
    }
  }
  
  /**
   * Create inverted index for all distinct rank elements
   */
  def getInvertedIndex(ranksArray: RDD[(Long, Array[Long])])
  : RDD[(Long, Iterable[(Long, (Long, Array[Long]))])] = {
    // Create one tuple for each element
    val tuples = ranksArray.flatMap(x => arrayToIdElement(x))
  
    // Group on element
    // Inverted index as: Array[element, (rank, element)*]
    // TODO: avoid element duplicated into each tuple on Iterable
    val invertedIndex = tuples.groupBy(tup => tup._1)    
    
    return invertedIndex
  }
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)

    val configXml = Args.configXml
    
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdx"
    var master = ((configXml \\ "config") \\ "masterIp").text
    var k = Args.k
    var storeCount = Args.COUNT    
    
    val threshold = Footrule.denormalizeThreshold(k, normThreshold)
    
    val conf = new SparkConf().setMaster(master).setAppName("bruteForce").set("spark.driver.allowMultipleContexts", "true")
    
    val sc = new SparkContext(conf)
    try {  
      // Partition ranks
      val ranksArray = Load.spaceSeparated(input, sc)
      
      val invertedIndex = getInvertedIndex(ranksArray)
      
      val flatInvIdx = invertedIndex.flatMap(x => x._2)
      
      // Filter to make cartesian only for: 1° arrays of same element, 2° smaller ID always on left (also avoid self join)
      val distinctCandidates = flatInvIdx.cartesian(flatInvIdx).filter(x => ((x._1._1 == x._2._1) && (x._1._2._1 < x._2._2._1))).map(x => (x._1._2, x._2._2)).distinct()
      
      val allDistances = distinctCandidates.map(x => Footrule.onLeftIdIndexedArray(x))
      
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