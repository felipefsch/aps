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
    def spaceSeparated ( path: String, sc: SparkContext )
    : RDD[(Long, Array[Long])] = {
      
      // File reading
      val file = sc.textFile(path)
      
      // Split elements
      val ranks = file.map(a => a.split(" ").map(_.toLong))
      
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
     * -RDD[Element1, Element2,...]
     * 
     * Load space separated ranking without ID
     */
    def spaceSeparatedNoID ( path: String, sc: SparkContext)
    : RDD[Array[Long]] = {
      
      // File reading
      val ranks = sc.textFile(path)
    
      // Partition ranks
      val ranksArray = ranks.map(b => b.split(" ").map(_.toLong))
      
      return ranksArray      
    }

    /**
     * Transform string (List[Elements], ID) into RDD tuple (Array[Elements], ID)
     */
    @Deprecated
    def listStringToTuple ( str: String, nElements: Int ) : (Array[Long], Long) = {
      val rankId = str.slice(str.indexOfSlice("),") + 2, str.size - 1)
      
      // String with comma + space separated elements.
      // Remove initial "(List(" and final "), rankId)" substrings
      var commaSeparated = str.slice(6, str.indexOfSlice("),"))
      
      var auxSize = 0
      var commaIndex = 0
      var element = ""

      // Create array of elements based on input string
      val elements = new Array[Long](nElements)
      for (i <- 0 until nElements - 1) {
        commaIndex = commaSeparated.indexOf(",")
        auxSize = commaSeparated.size 
        element = commaSeparated.slice(0, commaIndex)
        elements(i) = element.toLong
        
        // Removing already parsed element
        commaSeparated = commaSeparated.slice(commaIndex + 2, auxSize)
      }
      // Last element don't have comma on its end
      elements(nElements - 1) = commaSeparated.toLong
      
      return (elements, rankId.toLong)
    }    
    
}