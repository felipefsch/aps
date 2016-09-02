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
     * -(Element, Rank, RankingID)
     * 
     * For all ranking elements
     */
    def emitElementRankId[T1, T2]( in: (T1, Array[T2])) : Array[(T2, Long, T1)] = {
      var array = in._2
      var rankingId = in._1
      
      var output = Array.tabulate(array.length){ x => (array(x), x.toLong, rankingId)}
      
      return output
      /*for (i <- 0 until array.size) yield {
        (array(i), i.toLong, rankingId)
      }*/
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
    def emitCandidatePairs[T1 <%Ordered[T1], T2]( in: Iterable[(T2, Long, T1)])
    : Iterable[((T1, T1),(T2, Long, Long))] = {

      var output = in.flatMap(x => in.map(y => ((x._3, y._3),(x._1, x._2, y._2))).filter(x => x._1._1 < x._1._2))
      
      return output
      /*for (in1 <- in; in2 <- in; if (in1._3 < in2._3)) yield {
          ((in1._3, in2._3),(in1._1, in1._2, in2._2))  
      }*/
    }
  
    def main(args: Array[String]): Unit = {
      Args.parse(args)
     
      // For profiling
      var begin, end = 0.toLong
      val normThreshold = Args.normThreshold
      val input = Args.input    
      val output = Args.output + "ElementSplit"
      val master = Args.masterIp
      
      val conf = new SparkConf()
              .setMaster(master)
              .setAppName("elementSplit")
              .set("spark.driver.allowMultipleContexts", "true")
              .set("spark.executor.cores", Args.cores)
              .set("spark.executor.instances", Args.executors)
      
      val sc = new SparkContext(conf)
      
      try {
        // Load also sets ranking size k
        var ranks = Load.loadData(input, sc, Args.partitions)
        
        if (Args.PREGROUP)
          ranks = PreProcessing.groupDuplicatesAndStore(ranks, output)
        
        if (Args.DEBUG) {
          println("Minimum overlap: " + Args.minOverlap + " denormalized threshold: " + Args.threshold)
        }        
  
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
        val filteredOnOverlap = groupOnCandidates.filter(x => x._2.size >= Args.minOverlap.toInt)                                             
        
        // Compute final distance and filter on threshold       
        var similarRanks = filteredOnOverlap.map(x => Footrule.onPositionsWithPrediction(x, Args.threshold, Args.k))
                                            .filter(x => x._2 <= Args.threshold)                                       

      
        if (Args.PREGROUP) {
          var duplicates = PreProcessing.getDuplicate(ranks)
          similarRanks = similarRanks.union(duplicates)
        }

        // Saving output locally on each node     
        Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)        
        
      } finally {
        sc.stop()
      }
    }
}