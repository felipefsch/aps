package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File

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
    private def arrayToTuple[T: Manifest] ( in: Array[T]) : (T, Array[T]) = {
      var rankingID = in(0)
      var elements = new Array[T](in.size - 1)
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
    private def spaceSeparated(path: String, sc: SparkContext, partitions: Int)
    : RDD[(String, Array[String])] = {
      
      // File reading
      val file = sc.textFile(path, partitions)
      
      // Split elements
      val ranks = file.map(a => a.split(" "))
      
      // Set ranking size (When running on yarn, doesn't work. Set K in advance as argument)
      //Args.setK(ranks.first().size - 1)
      
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
    private def colonSeparated (path: String, sc: SparkContext, partitions: Int, k: Int, n: Int)
    : RDD[(String, Array[String])] = {
      // File reading
      val file = sc.textFile(path, partitions)

      // Split input, removing initial string and
      // elements as colon separated numbers
      val ranks = file.map(a => 
                           (a.substring(1, a.indexOf(",")),
                            a.substring(a.lastIndexOf("\t") + 1, a.length() - 1)
                             .split(":")
                             .slice(0, k)
                            )
                           )
                           
      // Filter on ranking size, pruning those that are smaller than desired
      var filtered = ranks.filter(x => x._2.size == k)
      
      if (n > 0) {
        // Take only desired amount of entries
        val filterAmount = filtered.take(n)
        //filtered = filtered.take(n)
        
        // Convert array to RDD
        filtered = sc.parallelize(filterAmount).repartition(partitions)
      }      
      
      return filtered
    }
    
    def loadData(path: String, sc: SparkContext, partitions: Int, k: Int, n: Int) 
    : RDD[(String, Array[String])] = {            
      // Analyze the first line of the input to check its format
      val src = sc.textFile(path)
      val line = src.take(1).mkString      
      
      var commaSeparated = false      
      if (line.contains(":"))
        commaSeparated = true
      
      if (commaSeparated)
        return this.colonSeparated(path, sc, partitions, k, n)
      else
        return this.spaceSeparated(path, sc, partitions)
    }
      
    /**
     * Input:
     * -dir: directory path
     * 
     * Get part-xxxxx files in directory and make string separating the
     * paths with semi colon (",")
     */
    private def getPartFiles(dir: String)
    : String = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList.filter(_.getAbsolutePath.contains("part-"))
            .filter(!_.getAbsolutePath.contains(".crc")).mkString(",")  
      } else {
        ""
      }
    }
    
    /**
     * Input:
     * -dir: directory with part files to be read 
     */
    def loadSimilars(dir: String, sc: SparkContext, partitions: Int)
    : RDD[(String, String)] = {
      val partFiles = getPartFiles(dir)
      var l = sc.textFile(partFiles, partitions)
      var similars = l.map(x =>
        (x.substring(x.lastIndexOf("(") + 1, x.indexOf(",")),
         x.substring(x.indexOf(",") + 1, x.indexOf(")")))
      )
      
      return similars
    }
}