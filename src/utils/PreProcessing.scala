package utils

import org.apache.spark.rdd.RDD

object PreProcessing {
  
  def stringToArray(input: String, separator: String = ":") : Array[Long] = {
    return input.split(separator).map(x => x.toLong)
  }
  
  /**
   * 
   */
  def groupDuplicates(inputRdd: RDD[(Long, Array[Long])])
  : RDD[(String, Array[Long])] = {
    
    // Transform array into string for easier merging
    val switched = inputRdd.map(x => (x._2.mkString(":"), x._1.toString()))
    
    // Group on equal arrays, concatenating keys
    val grouped = switched.reduceByKey((a,b) => (a.concat(":").concat(b)))
   
    // Switch back to original key/value format
    val output = grouped.map(x => (x._2, stringToArray(x._1)))
    
    return output
  }
}