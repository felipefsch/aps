package utils

import org.apache.spark.rdd.RDD

/**
 * Object for functions to fetch ranking to given IDs
 */
object Fetch {
  
  /**
   * Input:
   * -rdd: (ID, [Elements])*
   * -ids: ((ID1, ID2), Distance)
   * 
   * Output:
   * -((ID1, [Elements]), (ID2, [Elements])), Distance)
   */
  def fetchIds[T1, T2](rdd: RDD[(T1, Array[T2])], ids: RDD[((T1, T1), Long)]) 
  : RDD[(((T1, Array[T2]), (T1, Array[T2])), Long)] = {
    return ids.map(x =>
      ((rdd.filter(y => x._1 == y._1).first(), rdd.filter(y => x._2 == y._1).first()), x._2))
  }
  
  
  
}