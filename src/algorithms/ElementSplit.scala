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
import org.apache.log4j._

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
    
    def run(in: RDD[(String, Array[String])], threshold: Long, k: Int, minOverlap: Long)
    : RDD[((String, String), Long)] = {
        // Create (Element, Pos, ID)       
        val triples = in.flatMap(x => emitElementRankId(x))
        
        // Group on elements (Element, [Element, Pos, ID]*)
        // and remove element to get [Element, Pos, ID]*
        val groupOnElement = triples.groupBy(tup => tup._1).map(x => x._2)
        
        // Possible candidate pair for each element     
        val candidates = groupOnElement.flatMap(x => emitCandidatePairs(x))
        
        // Group elements for all created candidates     
        val groupOnCandidates = candidates.groupByKey()    
        
        // Filter empty candidates and those without minimum
        // overlap,since we know threshold can not be reached    
        val filteredOnOverlap = groupOnCandidates.filter(x => x._2.size >= minOverlap.toInt)                                             
        
        // Compute final distance and filter on threshold       
        var similarRanks = filteredOnOverlap.map(x => Footrule.onPositionsWithPrediction(x, threshold, k))
                                            .filter(x => x._2 <= threshold)
                                            
        return similarRanks
    }    
  
    def main(args: Array[String]): Unit = {
      Args.parse(args)
      
      // Variables not modifiable. Important when running on a cluster
      // so that all nodes have the correct values
      val output = Args.output + "ElementSplit"
      val masterIp = Args.masterIp
      val threshold = Args.threshold
      val normThreshold = Args.normThreshold
      val input = Args.input
      val k = Args.k
      val n = Args.n
      val minOverlap = Args.minOverlap
      val hdfsUri = Args.hdfsUri
      val partitions = Args.partitions
      val COUNT = Args.COUNT
      val DEBUG = Args.DEBUG
      val STORERESULTS = Args.STORERESULTS      
      val GROUPDUPLICATES = Args.GROUPDUPLICATES
      
      val sc = Config.getSparkContext(masterIp)
      
      try {
        // Load also sets ranking size k
        var ranksArray = Load.loadData(input, sc, partitions, k, n)
        
        var duplicates : org.apache.spark.rdd.RDD[((String, String), Long)] = sc.emptyRDD      
        if (GROUPDUPLICATES) {
          ranksArray = Duplicates.groupDuplicates(ranksArray)
          duplicates = Duplicates.getDuplicates(ranksArray)        
        }
        
        if (DEBUG) {
          println("Minimum overlap: " + minOverlap + " denormalized threshold: " + threshold)
        }        
                                                    
        var similarRanks = run(ranksArray, threshold, k, minOverlap)                                            

        if (GROUPDUPLICATES) {
          var rddUnion = similarRanks.union(duplicates)
          similarRanks = Duplicates.expandDuplicates(rddUnion)
        }
  
        Store.rdd(output, similarRanks, COUNT, STORERESULTS, hdfsUri)    
      } catch {
        case e:
          Exception => val log = LogManager.getRootLogger
          log.error(e.toString())
      } finally {
        Config.closeSparkContext(sc)
      }
    }
}