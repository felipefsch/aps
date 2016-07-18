package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.nio.file.Files
import java.nio.file.Paths
import scala.xml.XML
import utils._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.IndexedSeq

/**
 * Element Split algorithm:
 * -distribution based on ranking elements
 * -includes:
 * --filtering on the minimum overlap
 * --prediction of non overlapping elements distance
 */
object ElementSplit {
  
    private def DEBUG = false
  
    /**
     * Input:
     * -(ID, [Elements]*)
     * Output:
     * -(Element, Rank, ID)
     * 
     * For all ranking elements
     */
    def emitElementRankId( in: (Long, Array[Long])) : IndexedSeq[(Long, Long, Long)] = {
      var array = in._2
      var rankingId = in._1
      
      for (i <- 0 until array.size) yield {
        (array(i), i.toLong, rankingId)
      }
    }
    
    /**
     * Input:
     * -[(Element, Rank, ID)]*
     * Output:
     * -(ID1, ID2),(Element, Rank1, Rank2)
     * 
     * Where ID1 < ID2
     * Rank1 = Position of element on ranking ID1
     * Rank2 = Position of element on ranking ID2
     */    
    def emitCandidatePairs( in: Iterable[(Long, Long, Long)])
    : Iterable[((Long, Long),(Long, Long, Long))] = {

      for (in1 <- in; in2 <- in; if (in1._3 < in2._3)) yield {
          ((in1._3, in2._3),(in1._1, in1._2, in2._2))  
      }
    }
  
    def main(args: Array[String]): Unit = {
      Args.parse(args)
  
      val configXml = Args.configXml
      
      var normThreshold = Args.normThreshold
      var input = Args.input    
      var output = Args.output + "ElementSplit"
      var master = ((configXml \\ "config") \\ "masterIp").text
      var k = Args.k
      var storeCount = Args.COUNT      
      
      val minOverlap = Footrule.getMinOverlap(k, normThreshold)
      val threshold = Footrule.denormalizeThreshold(k, normThreshold)
      
      if (DEBUG) {
        println("Minimum overlap: " + minOverlap + " denormalized threshold: " + threshold)
      }
      
      val conf = new SparkConf().setMaster(master).setAppName("bruteForceFootrule")
      
      val sc = new SparkContext(conf)
      
      try {
        val ranks = Load.spaceSeparated(input, sc)
  
        // Create (Element, Pos, ID)
        val triples = ranks.flatMap(x => emitElementRankId(x))
        
        // Group on elements (Element, [Element, Pos, ID]*)
        // and remove element to get [Element, Pos, ID]*
        val groupOnElement = triples.groupBy(tup => tup._1).map(x => x._2)
        
        // Possible candidate pair for each element
        val candidates = groupOnElement.flatMap(x => emitCandidatePairs(x))
        
        // Group elements for all created candidates
        val groupOnCandidates = candidates.groupByKey()
        
        // Filter empty candidates and those without minimum
        // overlap,since we know threshold can not be reached
        val filteredOnOverlap = groupOnCandidates.map(x => if (x._2.size >= minOverlap.toInt) x)
                                                 .filter(x => x != ())
        
        // Compute final distance and filter on threshold
        val similarRanks = filteredOnOverlap.map(x => Footrule.onPositionsWithPrediction(x, threshold, k))
                                            .filter(x => x._2 <= threshold)
                                   
        // Saving output locally on each node
        if (storeCount)
          Store.rddToLocalAndCount(output, similarRanks)
        else
          Store.rddToLocalMachine(output, similarRanks)
        
      } finally {
        sc.stop()
      }
    }
}