package utils

import Array._

object Footrule {

  private def DEBUG = false
   
  /**
   * Defining factorial operation with operator '!'
   */
  implicit def pimp(n:Int) = new { def ! = ((1 to n) :\ 1) ( _ * _ ) }  
  
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
  
  // Sweet Spot between Inverted Indices and Metric-Space Indexing: Lemma 2
  def getMinOverlap ( k: Long, threshold: Long ) : Long = {
    var w = math.floor(0.5 * (1 + (2 * k) - (math.sqrt(1 + (4 * threshold)))))
    return k - math.round(w)
  }  
  
  def getMinOverlap ( k: Long, threshold: Double ) : Long = {
    return getMinOverlap(k, denormalizeThreshold(k, threshold))
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
  
  def onLeftIdIndexedArray[T1, T2] ( ranks:((T1, Array[T2]), (T1, Array[T2])))
  : ((T1,T1), Long) = {
    var footrule:Long = 0;
    val ranks1:Array[T2] = ranks._1._2
    val ranks2:Array[T2] = ranks._2._2
    
    // Both should have same k number of elements
    val k = ranks1.size
    
    // Check for elements intersection to compute distance
    // and sum contribution of elements only on first ranking
    for (i <- 0 until k) {
      var elPos = ranks2.indexOf(ranks1(i))
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
      var elPos = ranks1.indexOf(ranks2(i))
      if (elPos < 0)
        footrule = footrule + partialDistance(i, k)        
    }
    
    if (DEBUG) {
      println("Final footrule: " + footrule)
    }    

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
      k: Long)
  : ((String,String), Long) = {
    // Input as ANY should be casted!
    var in = in1.asInstanceOf[((String, String), Iterable[(String,Long,Long)])]
    
    // Pool with rank positions
    var pool1 = range(0, k.toInt)
    var pool2 = range(0, k.toInt)
    
    var ids = in._1
    var elements = in._2
    
    var footrule = 0.toLong;
    
    // Footrule of existing elements, removing elements from pools
    for (e <- elements) {
      // Removing values from pool by changing it to ranking size
      // this ways, doesn't contribute on costs later on
      pool1(e._2.toInt) = k.toInt
      pool2(e._3.toInt) = k.toInt
      
      // Adding partial costs
      footrule = footrule + partialDistance(e._2, e._3)
    }
    
    // Check if threshold reached to avoid traversing pools
    if (footrule < threshold) {
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