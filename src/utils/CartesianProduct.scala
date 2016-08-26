package utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Optimized cartesian product functions
 */
object CartesianProduct {
  
  /**
   * Input:
   * -in: RDD tuples with ranking ID as first element
   * 
   * Output:
   * -product: cartesian product of RDD elements without self product and no
   * symmetry, i.e., output (1,2) but not (2,1). Lower ID always on left.
   */
  def orderedWithoutSelf[T] ( in: RDD[(String, T)] ) : RDD[((String, T), (String, T))] = {
    
    val product = in.cartesian(in)
    val productFiltered = product.filter(x => x._1._1 < x._2._1)
    
    return productFiltered
  }
}