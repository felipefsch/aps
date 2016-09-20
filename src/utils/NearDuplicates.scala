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
    // If we convert to Long, we loose the ordering used
    // to output the similar pairs
    //var idsLong = ids.map(x => x.toLong)
    var sorted = ids.sorted
    return sorted.mkString(":")
  }
  
  /**
   * Input:
   * -in: (ID1:ID2, ID1:ID2:ID5:ID6)
   * 
   * Output:
   * -(ID1:ID2, 1)
   * 
   * Get the smallest of the pair (being a subset of the first)
   * and tag it as such
   */
  private def getSmallestSubset(in: (String, String))
  : (String, Int) = {
    // As one is subset of the other, length is enough to get smallest
    if (in._1.length() > in._2.length()) {
      return (in._2, 1)
    }
    else {
      return (in._1, 1)
    }
  }  
  
  def emitIds(pair: (String, String)) : IndexedSeq[String] = {
    for (i <- 0 until 2) yield {
      var first = true
      if (first) {
        first = false
        pair._1
      }
      else
        pair._2
    }
    
  }
  
  def getRepresentativeId(ids: String)
  : String = {
    return ids.split(":")(0)
  }
  
  def getLongerString(in1: String, in2: String)
  : String = {
    if (in1.length() > in2.length())
      return in1
    else
      return in2
  }
  
  def isSubset(in: (String, String)) : Boolean = {
    var ids1 = in._1.split(":")
    var ids2 = in._2.split(":")
    var aux = ids1
    
    // ids2 always as the smaller
    if (ids1.size < ids2.size) {
      ids1 = ids2      
      ids2 = aux
    }
    
    // Check if all elements from smaller contained on bigger
    var isSubset = true    
    for (i <- ids2) {
      if (!ids1.contains(i))
        isSubset = false
    }

    return isSubset
  }
  
  /**
   * Input:
   * -rdd: pairs of similar rankings (ID1, ID2)
   * 
   * Output:
   * -groupedIds: group them into clusters, smallest ID elements representing the group
   */
  def groupNearDuplicates(similars: RDD[(String, String)], allRankings: RDD[(String, Array[String])])
  : RDD[(String, Array[String])] = {
    // IDs to be removed from input dataset since they are already results    
    var similarIDs = similars.flatMap(x => Array(x._1, x._2)).distinct().map(x => (x, Array[String]()))
    
    // Input without IDs of similar rankings
    var filteredInput = allRankings.union(similarIDs)
                                   .reduceByKey((a, b) => Array[String]())
                                   .filter(f => !f._2.isEmpty)                                 
                                   
    // Grouped sets
    var grouped = similars.reduceByKey((a,b) => (a.concat(":").concat(b)))
                          .map(x => getOrderedConcatenation(x))
                          .map(x => (x, 0))
             
    // Group on first ID to get groups of similar pairs, ordering the ids
    var groupedOnFirst = similars.reduceByKey((a,b) => (a.concat(":").concat(b)))
                          .map(x => getOrderedConcatenation(x))
                          .map(x => (getRepresentativeId(x), (x, 0)))                           
                          
    // Group on second ID to get groups of similar pairs and ordering the ids
    var groupedOnSecond = similars.map(x => (x._2, x._1))
                          .reduceByKey((a,b) => (a.concat(":").concat(b)))
                          .map(x => getOrderedConcatenation(x))
                          .map(x => (getRepresentativeId(x), (x, 1)))                          
                          
    // Merge grouped IDs by the same representative ranking.
    // The set of IDs represented is the longest one found, since the others are all subsets
    // and we get only those present on both grouped, this way removing possible subsets
    var nearDuplicates = groupedOnFirst.union(groupedOnSecond)
                          .reduceByKey((a,b) => (getLongerString(a._1, b._1), a._2 + b._2))
                          .filter(f => f._2._2 > 0)
                          .map(x => x._2._1)                            
                                
    // Use first ID of merged IDs to fetch ranking to be used as representative to the set
    var duplicatesIdFetch = nearDuplicates.map(x => (x.substring(0, x.indexOf(":")), x.substring(x.indexOf(":"), x.length())))
                                          .join(allRankings)
                                          .map(x => (x._1.concat(x._2._1), x._2._2))                                             
                                          
    var inputWithNearDuplicates = filteredInput.union(duplicatesIdFetch)
    
    return inputWithNearDuplicates
  }
  
  def getNearDuplicates(duplicatesDir: String, allInputs: RDD[(String, Array[String])], sc: SparkContext, partitions: Int)
  : RDD[(String, Array[String])] = {    
    var similars = Load.loadSimilars(duplicatesDir, sc, partitions)
    return groupNearDuplicates(similars, allInputs)
  }
}