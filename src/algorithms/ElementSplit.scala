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

import benchmark.Profiling

/**
 * Element Split algorithm:
 * -distribution based on ranking elements
 * -includes:
 * --filtering on the minimum overlap
 * --prediction of non overlapping elements distance
 */
object ElementSplit {
  
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
     
      // For profiling
      var begin, end = 0.toLong
      var normThreshold = Args.normThreshold
      var input = Args.input    
      var output = Args.output + "ElementSplit"
      var master = Args.masterIp
      
      val conf = new SparkConf()
              .setMaster(master)
              .setAppName("elementSplit")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.executor.cores", Args.cores)
              .set("spark.executor.instances", Args.executors)
      
      val sc = new SparkContext(conf)
      
      try {
        // Load also sets ranking size k
        begin = System.nanoTime()
        val ranks = Load.spaceSeparated(input, sc, Args.partitions)
        end = System.nanoTime()
        Profiling.stageTime("load data", begin, end)
        
        if (Args.DEBUG) {
          println("Minimum overlap: " + Args.minOverlap + " denormalized threshold: " + Args.threshold)
        }        
  
        // Create (Element, Pos, ID)
        begin = System.nanoTime()        
        val triples = ranks.flatMap(x => emitElementRankId(x))
        end = System.nanoTime()
        Profiling.stageTime("create triples", begin, end)
        
        // Group on elements (Element, [Element, Pos, ID]*)
        // and remove element to get [Element, Pos, ID]*
        begin = System.nanoTime()        
        val groupOnElement = triples.groupBy(tup => tup._1).map(x => x._2)
        end = System.nanoTime()
        Profiling.stageTime("group on element", begin, end)
        
        // Possible candidate pair for each element
        begin = System.nanoTime()        
        val candidates = groupOnElement.flatMap(x => emitCandidatePairs(x))
        end = System.nanoTime()
        Profiling.stageTime("create pairs", begin, end)
        
        // Group elements for all created candidates
        begin = System.nanoTime()        
        val groupOnCandidates = candidates.groupByKey()
        end = System.nanoTime()
        Profiling.stageTime("group elements of pair", begin, end)        
        
        // Filter empty candidates and those without minimum
        // overlap,since we know threshold can not be reached
        begin = System.nanoTime()        
        val filteredOnOverlap = groupOnCandidates.map(x => if (x._2.size >= Args.minOverlap.toInt) x)
                                                 .filter(x => x != ())
        end = System.nanoTime()
        Profiling.stageTime("filter on overlap", begin, end)                                                 
        
        // Compute final distance and filter on threshold
        begin = System.nanoTime()        
        val similarRanks = filteredOnOverlap.map(x => Footrule.onPositionsWithPrediction(x, Args.threshold, Args.k))
                                            .filter(x => x._2 <= Args.threshold)
        end = System.nanoTime()
        Profiling.stageTime("compute final distance and filter", begin, end)                                            
                                   
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