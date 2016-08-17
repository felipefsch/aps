package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.io.Source

/**
 * Data Loading method.
 * All methods necessary to read input files are
 * implemented under this object 
 */
object Load {
    /**
     * Input:
     * -[RankingID, Element1, Element2,...]
     * 
     * Output:
     * -(RankingID, [Element1, Element2,...])
     */
    private def arrayToTuple ( in: Array[Long]) : (Long, Array[Long]) = {
      var rankingID = in(0)
      var elements = new Array[Long](in.size - 1)
      for (i <- 1 until in.size) {
        elements(i -1) = in(i)
      }      
      return (rankingID, elements)
    }

    /**
     * Input:
     * -path: the path to the input data set
     * -sc: the spark context
     * 
     * Output:
     * -RDD[(RankingID, [Element1, Element2,...])]
     * 
     * Load space separated ranking with ID as first element. It also sets
     * size of ranking K in order to prevent wrong input parameters usage or similar
     */
    def spaceSeparated ( path: String, sc: SparkContext, partitions: Int )
    : RDD[(Long, Array[Long])] = {
      
      // File reading
      val file = sc.textFile(path).repartition(partitions)
      
      // Split elements
      val ranks = file.map(a => a.split(" ").map(_.toLong))
      
      // Set ranking size
      Args.setK(ranks.first().size - 1)
      
      // Convert array into tuple of array elements and rank id
      val rankIdTuples = ranks.map(x => arrayToTuple(x))

      return rankIdTuples
    }
    
    /**
     * Input:
     * -path: the path to the input data set
     * -sc: the spark context
     * 
     * Output:
     * -RDD[(RankingID, [Element1, Element2,...])]
     * 
     * Load colon separated ranking and create unique IDs for the
     * loaded rankings.
     * 
     * ATENTION! - Ranking size MUST be provided in advance, since inputs
     * might have not uniform sizes
     */    
    def colonSeparated ( path: String, sc: SparkContext, partitions: Int )
    : RDD[(Long, Array[Long])] = {
      // File reading
      val file = sc.textFile(path).repartition(partitions)

      val ranks = file.map(a => 
                           a.substring(a.lastIndexOf("\t") + 1, a.length())
                            .split(":")
                            .slice(0, Args.k)
                            .map(_.toLong)
                           )
                           
      val filterSmall = ranks.filter(x => x.size == Args.k)

      val ranksWithId = filterSmall.zipWithUniqueId().map(x => (x._2, x._1))      
      
      return ranksWithId
    }
    
    def loadData( path: String, sc: SparkContext, partitions: Int ) 
    : RDD[(Long, Array[Long])] = {            
      // Analyze the first line of the input to check its format
      val src = Source.fromFile(path)
      val line = src.getLines.take(1).mkString      
      
      var commaSeparated = false      
      if (line.contains(":"))
        commaSeparated = true
      
      if (commaSeparated)
        return this.colonSeparated(path, sc, partitions)
      else
        return this.spaceSeparated(path, sc, partitions)
    }
}