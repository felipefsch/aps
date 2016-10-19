package utils

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

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
  def arrayToIdElement[T1, T2](in: (T1, Array[T2]), prefixSize: Long)
  : scala.collection.immutable.IndexedSeq[(T2, List[(T1, Array[T2])])] = {
    var id = in._1
    var rank = in._2
    var elements = in._2
    
    for (i <- 0 until prefixSize.toInt) yield {
      (elements(i), List((id, rank)))
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
  def getInvertedIndex[T1, T2: ClassTag](ranksArray: RDD[(T1, Array[T2])], prefixSize: Int)
  : RDD[(T2, List[(T1, Array[T2])])] = {
    // Create one tuple for each element
    val tuples = ranksArray.flatMap(x => arrayToIdElement(x, prefixSize))
  
    // Group on element
    // Inverted index as: Array[element, (rank, element)*]
    // TODO: avoid element duplicated into each tuple on Iterable
    //val invertedIndex = tuples.groupBy(tup => tup._1)
    val invertedIndex = tuples.reduceByKey((a, b) => a ++ b)
    
    return invertedIndex
  }
  
  /**
   * Given rank ID and its elements, create tuples as (rankId, element(i))
   * for all elements in the rank
   * 
   * Output:
   * -(Element, RankingID)
   */
  def arrayToIdPairs[T1, T2](in: (T1, Array[T2]), prefixSize: Int)
  : scala.collection.immutable.IndexedSeq[(T2, List[T1])] = {
    var id = in._1
    var elements = in._2
    
    for (i <- 0 until prefixSize) yield {
      (elements(i), List(id))
    }
  }
  
  /**
   * Input:
   * -[(ID, [Elements]*)]* 
   * Output:
   * -(Element, [Element, RankingID])
   */
  def getInvertedIndexIDs[T1, T2: ClassTag](ranksArray: RDD[(T1, Array[T2])], prefixSize: Int)
  : RDD[(T2, List[T1])] = {
    // Create one tuple for each element (id, element)
    val tuples = ranksArray.flatMap(x => arrayToIdPairs(x, prefixSize))
  
    // Inverted index as: Array[element, (rankId, element)*]
    // TODO: avoid element duplicated into each tuple on Iterable
    //val invertedIndex = tuples.groupBy(tup => tup._1)  
    val invertedIndex = tuples.reduceByKey((a, b) => a ++ b)
    
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
  def candidatesPerEntry[T1 <%Ordered[T1], T2](in: (T2, Iterable[(T1, Array[T2])]))
  : Iterable[Iterable[((T1, Array[T2]),(T1, Array[T2]))]] = {
    val element = in._1
    val rankings = in._2

    for (r1 <- rankings) yield {
      for (r2 <- rankings) yield {
        if (r1._1 < r2._1)
          ((r1._1, r1._2), (r2._1, r2._2))
        else
          ((r2._1, r2._2), (r1._1, r1._2)) 
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
  def getCandidates[T1 <%Ordered[T1], T2](in: RDD[(T2, List[(T1, Array[T2])])])
  : RDD[((T1, Array[T2]), (T1, Array[T2]))] = {
    var output = in.flatMap(x => candidatesPerEntry(x))
      .flatMap(x => x)
      .filter(p => (p._1._1 < p._2._1))
      .distinct()
      
    return output
  } 
  
  
  /**
   * Output:
   * -[RankingID1, RankingID2]  
   */
  def candidatesPerEntryIDs[T1 <%Ordered[T1], T2](in: (T2, Iterable[T1]))
  : Iterable[Iterable[(T1, T1)]] = {
    val element = in._1
    val ids = in._2
    
    for (r1 <- ids) yield {
      for (r2 <- ids) yield {
        if (r1 < r2)
          (r1, r2)
        else
          (r2, r1)  
      }
    }
  }    
  
  def getCandidatesIDs[T1 <%Ordered[T1], T2](in: RDD[(T2, List[T1])])
  : RDD[(T1, T1)] = {
    in.flatMap(x => candidatesPerEntryIDs(x))
      .flatMap(x => x)
      .filter(p => (p._1 < p._2))
      .distinct()
  }
}