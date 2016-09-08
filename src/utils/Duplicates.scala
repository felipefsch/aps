package utils

import org.apache.spark.rdd.RDD

object Duplicates {
  
  def stringToArray(input: String, separator: String = ":") : Array[String] = {
    return input.split(separator)
  }
  
  /*def arrayCombinations[T <%Ordered[T]](ar: Array[T], id2: T, dist: Long)
  : ((T, T), Long) = {
    
    var t = ar.map(x => x)
    
     for (a <- ar) yield {
       if (a < id2) {
         ((a, id2), dist)
       }
       else {
         ((id2, a), dist)
       }
     }
  }*/
  
  /**
   * Input:
   * -duplicates: (("id1:id2:id3:...", "duplicates"), 0)
   * 
   * Output:
   * -((id1, id2), 0)*
   * 
   * Expand duplicates into its pairs
   */
  def expandDuplicates(similarRanks: RDD[((String, String), Long)])
    : RDD[((String, String), Long)] = {
    // Expand duplicates, i.e., those with distance 0
    var filtered = similarRanks.filter(x => x._1._2.equals("duplicates"))
    var ids = filtered.map(x => x._1._1.split(":"))
    var pairs = ids.flatMap(x => CartesianProduct.orderedWithoutSelf(x))
    var expandedDuplicates = pairs.map(x => (x, 0.toLong))
    
    // Expand those with similar on left side
    var filteredLeft = similarRanks.filter(x => x._1._1.contains(":"))
    var idsLeft = filteredLeft.map(x => ((x._1._1.split(":"), x._1._2), x._2))
    var pairsLeft = idsLeft.map(x => x._1._1.map(y => if (x._2) else (x._2)))
    
    
    // Expand those with similar on right side
    var filteredRight = similarRanks.filter(x => x._1._2.contains(":"))
    var idsRight = filteredRight.map(x => ((x._1._2.split(":"), x._1._1), x._2))
    
    return pairs.map(x => (x, 0.toLong))
  }
  
  /**
   * After finding the duplicates, expand them
   */
  def getDuplicates[T](rdd: RDD[(String, Array[T])])
  : RDD[((String, String), Long)] = {
    return rdd.filter(x => x._1.contains(":")).map(x => ((x._1, "duplicates"), 0.toLong))  
  }

  /**
   * Input:
   * -inputRdd: (RankingID, [Elements])
   * -path: the path where to store duplicates
   * Output:
   * -(RankingID, [Elements])
   * 
   * Group duplicate rankings
   */
  def findDuplicates[T1, T2](inputRdd: RDD[(T1, Array[T2])], path: String)
  : RDD[(String, Array[String])] = {
    
    // Transform array into string for easier merging
    val switched = inputRdd.map(x => (x._2.mkString(":"), x._1.toString()))
    
    // Group on equal arrays, concatenating keys
    val grouped = switched.reduceByKey((a,b) => (a.concat(":").concat(b)))  
    
    // Switch back to original key/value format
    val output = grouped.map(x => (x._2, stringToArray(x._1)))
    
    return output
  }
}