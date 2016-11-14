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
     * -path: the path to the input data set
     * -sc: the spark context
     * 
     * Output:
     * -RDD[(RankingID, [Element1, Element2,...])]
     * 
     * Load space separated ranking with ID as first element. It also sets
     * size of ranking K in order to prevent wrong input parameters usage or similar
     */
    private def synthetic(path: String, sc: SparkContext, partitions: Int, k: Int, n: Int)
    : RDD[(String, Array[String])] = {
      // File reading
      val file = if (partitions != 0){ 
                   sc.textFile(path, partitions)
                 } 
                 else {
                   sc.textFile(path)
                 } 

      // Split input, removing initial string and
      // elements as colon separated numbers
      val ranks = file.map(a => 
                           (a.substring(0, a.indexOf(" ")),
                            a.substring(a.indexOf(" ") + 1, a.length())
                             .split(" ")
                             .slice(0, k)
                            )
                           )
                           
      // Filter on ranking size, pruning those that are smaller than desired
      var filtered = ranks.filter(x => x._2.size == k)   
      
      return filtered
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
    private def indexedNyt (path: String, sc: SparkContext, partitions: Int, k: Int)
    : RDD[(String, Array[String])] = {
      // File reading
      val file = if (partitions != 0){ 
                   sc.textFile(path, partitions)
                 } 
                 else {
                   sc.textFile(path)
                 } 

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
      
      return filtered
    }
    
    private def dblp (path: String, sc: SparkContext, partitions: Int, k: Int)
    : RDD[(String, Array[String])] = {
      // File reading
      val file = if (partitions != 0){ 
                   sc.textFile(path, partitions)
                 } 
                 else {
                   sc.textFile(path)
                 } 

      // Split input, removing initial string and
      // elements as colon separated numbers
      val ranks = file.map(a => 
                           (a.substring(0, a.indexOf(":")),
                            a.substring(a.indexOf(":") + 1, a.length())
                             .split(":")
                             .slice(0, k)
                            )
                           )
                           
      // Filter on ranking size, pruning those that are smaller than desired
      var filtered = ranks.filter(x => x._2.size == k)    
      
      return filtered
    }    
    
    /**
     * Take the first N entries from the RDD
     */
    private def subset[T](rdd: RDD[(String, Array[String])], n: Int, partitions: Int, sc: SparkContext)
    : RDD[(String, Array[String])] = {
      if (n > 0) {
        // Take only desired amount of entries
        val filterAmount = rdd.take(n)
        
        // Convert array to RDD
        if (partitions != 0){ 
          return sc.parallelize(filterAmount).repartition(partitions)
        } 
        else {
          return sc.parallelize(filterAmount)
        }
      }
      else {
        return rdd
      }
    }
    
    
    /**
     * Load file with input data. Able to distinguish automatically if
     * indexed NYT, DPLB or Synthetic data set input
     */
    def file(path: String, sc: SparkContext, partitions: Int, k: Int, n: Int) 
    : RDD[(String, Array[String])] = {            
      // Analyze the first line of the input to check its format
      val src = sc.textFile(path)
      val line = src.take(1).mkString      
      
      var isIndexedNyt = false      
      if (line.contains("("))
        isIndexedNyt = true
      
      var isDblp = false
      if (line.contains(":") & !isIndexedNyt)
        isDblp = true
        
      if (isIndexedNyt)
        return this.subset(this.indexedNyt(path, sc, partitions, k), n, partitions, sc)
      else if (isDblp)
        return this.subset(this.dblp(path, sc, partitions, k), n, partitions, sc)
      else
        return this.subset(this.synthetic(path, sc, partitions, k, n), n, partitions, sc)
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
    def similarPairs(dir: String, sc: SparkContext, partitions: Int)
    : RDD[(String, String)] = {
      val partFiles = getPartFiles(dir)
      var l = if (partitions != 0){ 
                   sc.textFile(partFiles, partitions)
                 } 
                 else {
                   sc.textFile(partFiles)
                 } 
      var similars = l.map(x =>
        (x.substring(x.lastIndexOf("(") + 1, x.indexOf(",")),
         x.substring(x.indexOf(",") + 1, x.indexOf(")")))
      )
      
      return similars
    }
}