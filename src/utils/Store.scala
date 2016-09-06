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
  
  var DELDIR = false
  
  /**
   * Input:
   * -path: folder path where to store number of elements
   * -rdd: the RDD with the local results
   * -count: count number of elements 
   * -storeRdd: store the rdd
   * 
   * Store RDD to defined path and count its elements if flag set to true
   */
  def rdd[T](
      path: String,
      rdd: RDD[T],
      count: Boolean,
      storeRdd: Boolean)
  : Unit = {
    if (storeRdd) {
      if (count) {
        rddToLocalAndCount(path, rdd)
      }
      else {
        rddToLocalMachine(path, rdd)
      }
    }
    else {
      if (count) {
        this.count(path, rdd)
      }
    }
  }
  
  /**
   * Input:
   * -path: folder path where to store number of elements
   * -rdd: the RDD with the local results
   * -count: count number of elements 
   * -storeRdd: store the rdd
   * -ids: rdd with pairs of ranking IDs and the distance between them
   * -fetchIds: fetch Ids before storing, so that we store complete ranking not only the ids
   * 
   * Store RDD to defined path and count its elements if flag set to true
   */
  def rdd[T1, T2](
      path: String,
      rdd: RDD[(T1, Array[T2])],
      count: Boolean,
      storeRdd: Boolean,
      ids: RDD[((T1, T1), Long)],
      fetchIds: Boolean)
  : Unit = {
    if (fetchIds) {
      var fetched = Fetch.fetchIds(rdd, ids)
      this.rdd(path, fetched, count, storeRdd)
    }
    else {
      this.rdd(path, rdd, count, storeRdd)
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
      val bw = new BufferedWriter(new FileWriter(file, true))      
      bw.write(rdd.count().toString() + "\n")
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
        if (new File(path).exists() && !DELDIR) {
          FileUtils.deleteDirectory(new File(path)); 
          DELDIR = true
        }
        rdd.saveAsTextFile(path)
      }
      catch {
        case NonFatal(t) => println("Could not output the result.")
      }
  }  
}