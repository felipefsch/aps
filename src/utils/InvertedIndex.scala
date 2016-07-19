package utils

import org.apache.spark.rdd.RDD

object InvertedIndex {
  
  /**
   * Input:
   * -(ID, [Elements]*)
   * -prefixSize
   * Output:
   * -[Element, (ID, [Elements]*)]*
   * 
   * Given rank ID and its elements, create tuples as
   * (rank, element(i)) for elements on ranking prefix
   */
  def arrayToIdElement(in: (Long, Array[Long]), prefixSize: Long)
  : scala.collection.immutable.IndexedSeq[((Long, Array[Long]), Long)] = {
    var id = in._1
    var rank = in._2
    var elements = in._2
    
    for (i <- 0 until prefixSize.toInt) yield {
      ((id, rank), elements(i))
    }
  }
  
  /**
   * Input:
   * -[(ID, [Elements]*)]*
   * Output:
   * -[(Element, (ID, [Elements]*))]*
   * 
   * Create inverted index ranking prefix
   */
  def getInvertedIndex(ranksArray: RDD[(Long, Array[Long])], prefixSize: Int)
  : RDD[(Long, Iterable[((Long, Array[Long]),Long)])] = {
    // Create one tuple for each element
    val tuples = ranksArray.flatMap(x => arrayToIdElement(x, prefixSize))
  
    // Group on element
    // Inverted index as: Array[element, (rank, element)*]
    // TODO: avoid element duplicated into each tuple on Iterable
    val invertedIndex = tuples.groupBy(tup => tup._2)    
    
    return invertedIndex
  }
  
  /**
   * Given rank ID and its elements, create tuples as (rankId, element(i))
   * for all elements in the rank
   */
  def arrayToIdPairs(in: (Long, Array[Long]), prefixSize: Int)
  : scala.collection.immutable.IndexedSeq[(Long, Long)] = {
    var id = in._1
    var elements = in._2
    
    for (i <- 0 until prefixSize) yield {
      (id, elements(i))
    }
  }
  
  /**
   * Create inverted index for all distinct rank elements
   */
  def getInvertedIndexIds(ranksArray: RDD[(Long, Array[Long])], prefixSize: Int)
  : RDD[(Long, Iterable[(Long,Long)])] = {
    // Create one tuple for each element (id, element)
    val tuples = ranksArray.flatMap(x => arrayToIdPairs(x, prefixSize))
  
    // Inverted index as: Array[element, (rankId, element)*]
    // TODO: avoid element duplicated into each tuple on Iterable
    val invertedIndex = tuples.groupBy(tup => tup._2)    
    
    return invertedIndex
  }
  
  /**
   * Input:
   * -(Element, [(Element, (RankingID, [Ranking]))]
   * 
   * Output:
   * -((RankingID, [Ranking]), (RankingID, [Ranking]))
   * 
   * Combine all pairs of element rankings for same element in the index
   */
  def candidatesPerEntry(in: (Long, Iterable[(Long, (Long, Array[Long]))])) = {
    val element = in._1
    val rankings = in._2

    for (r1 <- rankings) yield {
      for (r2 <- rankings) yield {
        if (r1._2._1 < r2._2._1)
          (r1._2, r2._2)
        else
          (r2._2, r1._2)  
      }
    }
  }   
  
  /**
   * Input:
   * -(Element, [Element, (RankingID, [Ranking])]
   * 
   * Output:
   * -((RankingID, [Ranking]),(RankingID, [Ranking]))
   * 
   * Given inverted index, generate candidate pairs
   */
  def getCandidates(in: RDD[(Long, Iterable[(Long, (Long, Array[Long]))])]) {
    in.flatMap(x => candidatesPerEntry(x)).flatMap(x => x).filter(p => (p._1._1 < p._2._1)).distinct()
  } 
}