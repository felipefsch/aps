package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

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
     * Load space separated ranking with ID as first element
     */
    def spaceSeparated ( path: String, sc: SparkContext, partitions: Int )
    : RDD[(Long, Array[Long])] = {
      
      // File reading
      val file = sc.textFile(path).repartition(partitions)
      
      // Split elements
      val ranks = file.map(a => a.split(" ").map(_.toLong))
      
      // Convert array into tuple of array elements and rank id
      val rankIdTuples = ranks.map(x => arrayToTuple(x))

      return rankIdTuples
    }
}