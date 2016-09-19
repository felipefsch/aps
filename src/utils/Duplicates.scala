package utils

import org.apache.spark.rdd.RDD

object Duplicates {
  
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
  def expandDuplicates(similarRanks: RDD[((String, String), Long)])
    : RDD[((String, String), Long)] = {
    var noDuplicates = similarRanks.filter(x => !x._1._1.contains("=") && !x._1._2.contains("="))    
    
    // Expand those with similar on left side
    var filteredLeft = similarRanks.filter(x => x._1._1.contains("=") && !x._1._2.equals("duplicates"))
    var expandedLeft = filteredLeft.flatMap(
                                x => x._1._1.split("=").map(
                                    y => 
                                      if (y < x._1._2 || x._1._2.contains("="))
                                        ((y, x._1._2), x._2)
                                      else
                                        ((x._1._2, y), x._2)
                                )
                              )  
    var expandedLeftOnly = expandedLeft.filter(x => !x._1._2.contains("="))
    
    // Expand those with similar on right side
    var filteredRight = similarRanks.filter(x => x._1._2.contains("="))
                                       .union(expandedLeft.filter(x => x._1._2.contains("=")))
    var expandedRight = filteredRight.flatMap(
                                x => x._1._2.split("=").map(
                                    y => 
                                      if (y < x._1._1)
                                        ((y, x._1._1), x._2)
                                      else 
                                        ((x._1._1, y), x._2)
                                )
                              )
    
     
    // Expand duplicates, i.e., those with distance 0
    var filtered = similarRanks.filter(x => x._1._2.equals("duplicates"))
    var ids = filtered.map(x => x._1._1.split("="))
    var pairs = ids.flatMap(x => CartesianProduct.orderedWithoutSelf(x))
    var expandedDuplicates = pairs.map(x => (x, 0.toLong))
    
    return expandedDuplicates.union(expandedRight).union(expandedLeftOnly).distinct()
  }
  
  /**
   * After finding the duplicates, expand them
   */
  def getDuplicates[T](rdd: RDD[(String, Array[T])])
  : RDD[((String, String), Long)] = {
    return rdd.filter(x => x._1.contains("=")).map(x => ((x._1, "duplicates"), 0.toLong))  
  }
  
  /**
   * Input:
   * -inputRdd: (RankingID, [Elements])
   * -path: the path where to store duplicates
   * Output:
   * -(RankingIDs, [Elements])
   * 
   * Group duplicate rankings, creating ID as "=ID1:ID2:ID3=" for the group
   */
  def groupDuplicates[T1, T2](inputRdd: RDD[(T1, Array[T2])], path: String)
  : RDD[(String, Array[String])] = {
    
    // Transform array into string for easier merging
    val switched = inputRdd.map(x => (x._2.mkString(":"), x._1.toString()))
    
    // Group on equal arrays, concatenating keys with "="
    val grouped = switched.reduceByKey((a,b) => (a.concat("=").concat(b)))  
    
    // Switch back to original key/value format
    val output = grouped.map(x => (x._2, stringToArray(x._1)))
    
    return output
  }
}