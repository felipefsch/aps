package utils

import Array._
import org.apache.log4j._

object Footrule {
  
  var DEBUG = false
   
  /**
   * Defining factorial operation with operator '!'
   */
  implicit def pimp(n:Int) = new { def ! = ((1 to n) :\ 1) ( _ * _ ) }  
  
  /**
   * The distances are ordered
   */
  def getMedoidDistances[T1, T2](ranking: (T1, Array[T2]), medoids: Array[(T1, Array[T2])])
  : Array[(T1, Long)] = {
    // TODO: IF THEY ALL HAVE THE SAME DISTANCE, TAKE ONE AT RANDOM FOR BETTER PARTITIONING!!!!
    val distances = medoids.map(x => (x._1, getRankingsDistance(ranking._2, x._2))).sortBy(_._2)
    
    return distances
  }
  
  /**
   * Given Array of medoids, get closes medoid to the ranking
   */
  def getClosestMedoid[T1, T2](distances: Array[(T1, Long)])
  : (T1, Long) = {
    // TODO: IF THEY ALL HAVE THE SAME DISTANCE, TAKE ONE AT RANDOM FOR BETTER PARTITIONING!!!!
    return distances.minBy(_._2)    
  }
  
  def getMaxDistance ( k: Long ) : Long = {
    return k * (k + 1) 
  }
  
  // ALWAYS 1!!!
  def getNormalizedMaxDistance ( k: Long ) : Double = {
    return normalizeThreshold(k, getMaxDistance(k)) 
  }
  
  def normalizeThreshold ( k: Long, threshold: Long ) : Double = {
    return (threshold.toDouble / (getMaxDistance(k)).toDouble)
  }
  
  def denormalizeThreshold ( k: Long, threshold: Double ) : Long = {
    return math.round((threshold * getMaxDistance(k)))
  }
  
  /**
   *  We want to be sure that in the prefix we have at least one
   *  common element between two rankings
   */
  def getPrefixSize( k: Long, threshold: Long ) : Long = {
    var prefixSize = 0.toLong 
   
    if (threshold <= (Math.pow(k, 2) / 2).toLong) {
      prefixSize = Math.floor(Math.sqrt(threshold) / Math.sqrt(2)).toLong + 1
    }
    else {
      prefixSize = k - getMinCommonElements(k, threshold) + 1
    }
    
    if (prefixSize > k)
      prefixSize = k
      
    return prefixSize
    
  }
  
  /**
   *  Sweet Spot between Inverted Indices and Metric-Space Indexing: Lemma 2
   *  Corrected to ceil instead of floor after talk to evica
   */
  def getMinCommonElements ( k: Long, threshold: Long ) : Long = {
    var w = math.ceil(0.5 * (1 + (2 * k) - (math.sqrt(1 + (4 * threshold)))))
    return math.round(w)
  }  
  
  def getMinCommonElements ( k: Long, threshold: Double ) : Long = {
    return getMinCommonElements(k, denormalizeThreshold(k, threshold))
  }
  
  /**
   *  Get k - w as in lemma 2 from paper "Sweet Spot between...."
   *  just to avoid changes on synthetic data set, which was working properly
   *  using such overlap (bigger than the updated version)
   */
  def getOverlapSyntheticDataSet( k: Long, threshold: Double ) : Long = {
    return k - getMinCommonElements(k, threshold)
  }
  
  def getOverlapSyntheticDataSet( k: Long, threshold: Long ) : Long = {
    return k - getMinCommonElements(k, threshold)
  }  
  
  /**
   * Input:
   * -(Pos1, Pos2)
   * Output:
   * -Partial Footrule distance
   * 
   * Given position of one element in two rankings,
   * compute the partial footrule distance
   */
  def partialDistance(in: (Long, Long)) : Long = {
    val dist =  math.abs(in._1 - in._2)
    
    if (DEBUG) {
      println("Partial distance: " + in._1 + " - " + in._2 + " = " + dist)
    }
    
    return dist
  }
  
  
  /**
   * Compute Footrule distance between two rankings arrays
   */
  def getRankingsDistance[T](ranking1: Array[T], ranking2: Array[T])
  : Long = {
    var footrule:Long = 0;
    
    // Both should have same k number of elements
    val k = ranking1.size
    
    // Check for elements intersection to compute distance
    // and sum contribution of elements only on first ranking
    for (i <- 0 until k) {
      var elPos = ranking2.indexOf(ranking1(i))
      if (elPos < 0)
        footrule = footrule + partialDistance(i, k)
      else
        footrule = footrule + partialDistance(i, elPos)        
    }
    
    if (DEBUG) {
      println("Ranking size: " + k)
      println("Partial footrule from ranking 1: " + footrule)
    }
    
    // Elements only on second ranking should
    // also contribute to Footrule distance
    for (i <- 0 until k) {
      var elPos = ranking1.indexOf(ranking2(i))
      if (elPos < 0)
        footrule = footrule + partialDistance(i, k)        
    }    
    
    if (DEBUG) {
      println("Final footrule: " + footrule)
    }       
    
    return footrule
  }
  
  def onLeftIdIndexedArray[T1, T2] ( ranks:((T1, Array[T2]), (T1, Array[T2])))
  : ((T1,T1), Long) = {
    var footrule = getRankingsDistance(ranks._1._2, ranks._2._2)

    return ((ranks._1._1, ranks._2._1), footrule)
  }
  
  /**
   * Input:
   * -Any, casted to ((ID1, ID2), [Element, Pos1, Pos2]*)
   * -Threshold
   * -k
   * Output:
   * -((ID1, ID2), FootruleDistance)
   * 
   * ID = Ranking ID
   * Element = Ranked element
   * Pos1 = Position of element on ranking ID1
   * Pos2 = Position of element on ranking ID2
   * Threshold = Footrule distance threshold for earlier termination
   * k = Size of ranking
   * 
   * Compute the distance between two rankings with the given elements,
   * predicting contribution to the distance of the elements not existing
   * on the given list
   */
  def onPositionsWithPrediction[T1, T2](
      in1: Any,
      threshold: Long,
      k: Int)
  : ((String,String), Long) = {
    // Input as ANY should be casted!
    var in = in1.asInstanceOf[((String, String), Iterable[(String,Long,Long)])]
    
    // Pool with rank positions
    var pool1 = range(0, k)
    var pool2 = range(0, k)

    var ids = in._1
    var elements = in._2
    
    var footrule = 0.toLong;
    
    // Footrule of existing elements, removing elements from pools
    for (e <- elements) {
      
      if (DEBUG) {
        val log = LogManager.getRootLogger
        log.info("Pos1: " + e._2.toInt + " pos2: " + e._3.toInt + " pool size: " + pool1.length + " k: " + k)
      }
      
      // Removing values from pool by changing it to ranking size
      // this ways, doesn't contribute on costs later on
      pool1(e._2.toInt) = k.toInt
      pool2(e._3.toInt) = k.toInt
      
      // Adding partial costs
      footrule = footrule + partialDistance(e._2, e._3)
    }
    
    // Check if threshold reached to avoid traversing pools
    if (footrule <= threshold) {
      for (p <- pool1) {
        footrule = footrule + partialDistance(p, k)
      }
      for (p <- pool2) {
        footrule = footrule + partialDistance(p, k)
      }
    } else {
      footrule = getMaxDistance(k)
    }
    return (ids, footrule)
  }
}