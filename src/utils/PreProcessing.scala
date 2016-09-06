package utils

import org.apache.spark.rdd.RDD

object PreProcessing {
  
  def stringToArray(input: String, separator: String = ":") : Array[String] = {
    return input.split(separator)
  }
  
  /**
   * Input:
   * -duplicates: (("id1:id2:id3:...", "duplicates"), 0)
   * 
   * Output:
   * -((id1, id2), 0)*
   * 
   * Expand duplicates into its pairs
   */
  def expandDuplicates(duplicates: RDD[((String, String), Long)])
    : RDD[((String, String), Long)] = {
    var ids = duplicates.map(x => x._1._1.split(":"))
    var pairs = ids.flatMap(x => CartesianProduct.orderedWithoutSelf(x))
    return pairs.map(x => (x, 0.toLong))
  }
  
  def getDuplicate[T](rdd: RDD[(String, Array[T])])
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
  def groupDuplicatesAndStore[T1, T2](inputRdd: RDD[(T1, Array[T2])], path: String)
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