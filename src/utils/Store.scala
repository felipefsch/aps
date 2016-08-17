package utils

import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD

import org.apache.commons.io.FileUtils
import java.io._
import java.io.IOException

/**
 * Data Storing methods.
 * All methods necessary to store results are
 * implemented under this object 
 */
object Store {
  
  /**
   * Input:
   * -path: folder path where to store number of elements
   * -rdd: the RDD with the local results
   * -count: count number of elements 
   * 
   * Store RDD to defined path and count its elements if flag set to true
   */
  def storeRdd[T]( path: String, rdd: RDD[T], count: Boolean) : Unit = {
    if (count) {
      rddToLocalAndCount(path, rdd)
    }
    else {
      rddToLocalMachine(path, rdd)
    }
      
  }
  
  /**
   * Input:
   * -path: folder path where to store number of elements
   * -rdd: the RDD with the local results
   * 
   * Store RDD and count its elements
   */
  private def rddToLocalAndCount[T]( path: String, rdd: RDD[T] ) : Unit = {
    rddToLocalMachine(path, rdd)
    count(path, rdd)
  }
  
  /**
   * Input:
   * -path: folder path where to store number of elements
   * -rdd: the RDD with the local results
   * 
   * This stores file "count.txt" under the specified folder
   * with the number of elements on the provided RDD
   */
  private def count[T]( path: String, rdd: RDD[T] ) : Unit = {
    
    var countPath = ""
    if (path.last.compare('/') == 0)
      countPath = path + "count.txt"
    else
      countPath = path + "/count.txt"
      
    var file = new File(countPath)
      
    try {     
      val bw = new BufferedWriter(new FileWriter(file))      
      bw.write(rdd.count().toString())
      bw.close()
      
    }
    catch {
      case NonFatal(t) => println("Could not store the amount of RDD elements.")
    }
    
  }
    
  /**
   * Input:
   * -path: folder path where to store local results
   * -rdd: the RDD with the local results
   * 
   * This stores files as part-0000... under the specified folder
   * where each line represent one RDD element
   */
  private def rddToLocalMachine[T]( path: String, rdd: RDD[T] ) : Unit = {
      try {
        if (new File(path).exists()) {
          FileUtils.deleteDirectory(new File(path)); 
        }
        rdd.saveAsTextFile(path)
      }
      catch {
        case NonFatal(t) => println("Could not output the result.")
      }
  }  
}