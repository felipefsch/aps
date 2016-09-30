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
   * -similarRanks: ((ID1:ID2,ID3:ID4), dist)
   * 
   * Output:
   * -((ID1,ID3), dist), ((ID1,ID4), dist), ((ID2,ID3), dist), ((ID2,ID4), dist)
   * 
   * Expand near duplicate pairs into its actual pairs
   * !!! Real distance "dist" will be wrong since distance considered only between
   * representatives!!! Expand ALL ONLY IF distance <= theta - theta_c, so that
   * is guaranteed the distance of all possible pairs of the near duplicates < theta
   */
  def expandAll(similarRanks: RDD[((String, String), Long)])
  : RDD[((String, String), Long)] = {
    // Expand those with similar on left side
    var filteredLeft = similarRanks.filter(x => x._1._1.contains(":"))
 
    var expandedLeft = filteredLeft.flatMap(
                                x => x._1._1.split(":").map(
                                    y => 
                                      if (y < x._1._2 || x._1._2.contains(":"))
                                        ((y, x._1._2), x._2)
                                      else
                                        ((x._1._2, y), x._2)
                                )
                              )  
                              
    // Entries expanded on left side of ID pair with nothing to be expanded on right side
    var expandedLeftOnly = expandedLeft.filter(x => !x._1._2.contains(":"))
    
    // Expand those with similar on right side
    var filteredRight = similarRanks.filter(x => x._1._2.contains(":") && !x._1._1.contains(":"))
                                    .union(expandedLeft.filter(x => x._1._2.contains(":")))
                                   
    var expandedRight = filteredRight.flatMap(
                                x => x._1._2.split(":").map(
                                    y => 
                                      if (y < x._1._1)
                                        ((y, x._1._1), x._2)
                                      else 
                                        ((x._1._1, y), x._2)
                                )
                              )
                              
    return expandedRight.union(expandedLeftOnly).distinct()
  }
  
  /**
   * Input:
   * -similarRanks: pairs of similar rankings
   * 
   * Output:
   * -pairs of similar rankings, excluding those that are not near
   * duplicates (which requires further processing) or have distance
   * greater than theta
   */
  def filterFalseCandidates(similarRanks: RDD[((String, String),Long)])
  : RDD[((String, String),Long)] = {
    // Remove pairs that have no near duplicates and higher threshold than desired
    return similarRanks.filter(f => f._2 < Args.threshold || f._1._1.contains(":") || f._1._2.contains(":"))
  }
  
  /**
   * Input:
   * -similarRanks: similar ranking pairs with max distance theta + theta_c
   * -allRanks: rankings without near duplicates grouping (and with duplicates grouping
   * if that is the case)
   * 
   * Output:
   * -rankings pairs with maximum distance theta
   */
  def expandNearDuplicates(similarRanks: RDD[((String, String), Long)],
                           allRanks: RDD[(String, Array[String])])
  : RDD[((String, String), Long)] = {   
    var noDuplicates = similarRanks.filter(x => !x._1._1.contains(":") && !x._1._2.contains(":"))    
    
    // Pairs containing near duplicates
    var withNearDuplicates = similarRanks.filter(f => f._1._1.contains(":") || f._1._2.contains(":"))

    // theta - theta_c
    var maxDist = Footrule.denormalizeThreshold(Args.k, Args.normThreshold - Args.normThreshold_c)

    // If dist <= theta - theta_c we can be sure that all pairs satisfy dist <= theta
    var toExpand = withNearDuplicates.filter(f => f._2 <= maxDist)    
    var expanded = expandAll(toExpand)
    
    // If  theta - theta_c < dist <= theta + theta_c, elements from representative might be candidates
    // or not! Need to fetch real ranking to search for correct distance.
    // Note that we might have duplicates here (e.g., ID1=ID2 as key)! Input allRanks must have
    // such entries as IDs as well!
    var toCheck = similarRanks.filter(f => f._2 > maxDist && (f._1._1.contains(":") || f._1._2.contains(":")))    
    var expandedToCheck = expandAll(toCheck).map(x => (x._1._1, x._1._2))
    
    // Join on rankId1 and transform output to (rankId2, (rankId1, (elements1))
    val firstJoin = allRanks.join(expandedToCheck).map(x => (x._2._2, (x._1, x._2._1)))
    // Join on rankId2 and transform output to ((rankId1, elements1), (rankId2, elements2)) 
    val secondJoin = allRanks.join(firstJoin).map(x => (x._2._2, (x._1, x._2._1)))
  
    var checked = secondJoin.map(x => Footrule.onLeftIdIndexedArray(x))      
    checked = checked.filter(x => x._2 <= Args.threshold)    
        
    return noDuplicates.union(expanded).union(checked)
  }
  
  /**
   * Input:
   * -similars: pairs of similar rankings (ID1, ID2)
   * -allRankings: all input rankings as (ID, [Elements*])
   * 
   * Output:
   * -groupedIds: group near duplicate rankings (ID1:ID2*, [Elements*]), removing
   * their IDs from the the set 
   * 
   */
  def groupNearDuplicates(similars: RDD[(String, String)], allRankings: RDD[(String, Array[String])])
  : RDD[(String, Array[String])] = {
    // IDs to be removed from input dataset since they are already results    
    var similarIDs = similars.flatMap(x => Array(x._1, x._2)).distinct().map(x => (x, Array[String]()))
    
    // Input without IDs of similar rankings
    var filteredInput = allRankings.union(similarIDs)
                                   .reduceByKey((a, b) => Array[String]())
                                   .filter(f => !f._2.isEmpty)
             
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