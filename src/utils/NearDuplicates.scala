package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object NearDuplicates {
 
  /**
   * Input:
   * -in: pair containing ids where the second are colon separated ids
   * 
   * Ordering based on String!
   */
  private def getOrderedConcatenation(in: (String, String))
  : String = {
    var ids = in._2.split(":") :+ in._1
    var idsLong = ids.map(x => x.toLong)
    var sorted = idsLong.sorted
    return sorted.mkString(":")
  }
  
  private def getSmallestSubset(in: (String, String))
  : String = {
    if (in._1.length() > in._2.length()) {
      return in._2
    }
    else {
      return in._1
    }
  }
  
  private def getBiggestSubset(in: (String, String))
  : String = {
    if (in._1.length() < in._2.length()) {
      return in._2
    }
    else {
      return in._1
    }
  }  
  
  /**
   * Input:
   * -rdd: pairs of similar rankings (ID1, ID2)
   * 
   * Output:
   * -groupedIds: group them into clusters, smallest ID representing the group
   */
  def getNearDuplicates(rdd: RDD[(String, String)])
  : RDD[String] = {
    var grouped = rdd.reduceByKey((a,b) => (a.concat(":").concat(b))).map(x => getOrderedConcatenation(x))
    
    // Cartesian product necessary to search for subsets
    var cartesian = CartesianProduct.orderedWithoutSelf(grouped)
    
    // Entries that are subset of the others
    var filtered = cartesian.filter(x => x._1.contains(x._2) || x._2.contains(x._1))
    
    var groupedNearDuplicates = filtered.map(x => getBiggestSubset(x))
     
    // The small ones should be removed from "grouped", which will be the result then
    var subsets = filtered.map(x => getSmallestSubset(x))    
    
    return groupedNearDuplicates
  }
  
  def getNearDuplicates(dir: String, sc: SparkContext, partitions: Int)
  : RDD[String] = {    
    var similars = Load.loadSimilars(dir, sc, partitions)
    return getNearDuplicates(similars)
  }
}