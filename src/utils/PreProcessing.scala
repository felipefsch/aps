package utils

import org.apache.spark.rdd.RDD

object PreProcessing {
  
  def stringToArray(input: String, separator: String = ":") : Array[String] = {
    return input.split(separator)
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
    
    // Store duplicates!
    val duplicates = grouped.filter(x => x._2.contains(":")).map(x => (x._2, 0))
    Store.storeRdd(path, duplicates, Args.COUNT)
    
    println("DUPLICATES")
    duplicates.map(println(_)).collect()
    
    // Switch back to original key/value format
    val output = grouped.map(x => (x._2, stringToArray(x._1)))
    
    return output
  }
}