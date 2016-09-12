package utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Optimized cartesian product functions
 */
object CartesianProduct {
  
  def orderedWithoutSelf(in: RDD[(String)])
  : RDD[(String, String)] = {
    val product = in.cartesian(in)   
    val productFiltered = product.filter(x => x._1 < x._2)
    
    return productFiltered
  }
  
  /**
   * Input:
   * -in: RDD tuples with ranking ID as first element
   * 
   * Output:
   * -product: cartesian product of RDD elements without self product and no
   * symmetry, i.e., output (1,2) but not (2,1). Lower ID always on left.
   */
  def orderedWithoutSelf[T1 <%Ordered[T1], T2] ( in: RDD[(T1, T2)] )
  : RDD[((T1, T2), (T1, T2))] = {   
    val product = in.cartesian(in)
    val productFiltered = product.filter(x => x._1._1 < x._2._1)
    
    return productFiltered
  }
  
  def orderedWithoutSelf[T <%Ordered[T]] ( in: Array[T] ) : Array[(T, T)] = {
    return in.flatMap(x => in.map(y => (x, y))).filter(f => f._1 < f._2)
  }
}