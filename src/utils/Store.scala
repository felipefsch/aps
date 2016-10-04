package utils

import scala.util.control.NonFatal

import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._
import java.util._
import java.net._

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
  var DEBUG = false
  
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
      storeRdd: Boolean,
      hdfsUri: String)
  : Unit = {
    if (storeRdd) {
      if (count) {
        rddToLocalAndCount(path, rdd, hdfsUri)
      }
      else {
        rddToLocalMachine(path, rdd, hdfsUri)
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
  /*@deprecated
  def rdd[T1, T2](
      path: String,
      allRanks: RDD[(T1, Array[T2])],
      count: Boolean,
      storeRdd: Boolean,
      similarRanks: RDD[((T1, T1), Long)],
      fetchIds: Boolean)
  : Unit = {
    if (fetchIds) {
      var fetched = Fetch.fetchIds(allRanks, similarRanks)
      this.rdd(path, fetched, count, storeRdd)
    }    
    else {
      this.rdd(path, similarRanks, count, storeRdd)
    }
  }*/
  
  /**
   * Input:
   * -path: folder path where to store number of elements
   * -rdd: the RDD with the local results
   * 
   * Store RDD and count its elements
   */
  private def rddToLocalAndCount[T]( path: String, rdd: RDD[T], hdfsUri: String) : Unit = {
    rddToLocalMachine(path, rdd, hdfsUri)
    count(path, rdd, hdfsUri)
  }
  
  /**
   * Input:
   * -path: folder path where to store number of elements
   * -rdd: the RDD with the local results
   * 
   * This stores file "count.txt" under the specified folder
   * with the number of elements on the provided RDD
   */
  private def count[T]( path: String, rdd: RDD[T], hdfsUri: String = "" ) : Unit = {
    
    val count = rdd.count()
    
    var countPath = ""
    if (path.last.compare('/') == 0)
      countPath = path + "count.txt"
    else
      countPath = path + "/count.txt"
    
    try {
      if (path.contains("hdfs")) {
        val conf = new Configuration()
        val hdfs = org.apache.hadoop.fs.FileSystem.get( new URI( hdfsUri ), conf );
        val file = new Path(countPath);
        
        hdfs.deleteOnExit(file)
        
        val os = hdfs.create(file)
        val br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
        br.write(count.toString() + "\n");
        br.close();
        hdfs.close();
      }
      else {   
        var file = new File(countPath)      
        val bw = new BufferedWriter(new FileWriter(file, true))       
        bw.write(count.toString() + "\n")
        bw.close()
      }
    }
    catch {
      case NonFatal(t) => println("[ERROR] Could not store the amount of RDD elements. " + t.toString() + "\n")
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
  private def rddToLocalMachine[T]( path: String, rdd: RDD[T], hdfsUri: String ) : Unit = {
      try {
        // First delete existing directory/file
        if (path.contains("hdfs")) {
          val conf = new Configuration()
          val hdfs = org.apache.hadoop.fs.FileSystem.get( new URI( hdfsUri ), conf );
          val file = new Path(path);          
          hdfs.deleteOnExit(file) 
          hdfs.close()          
        }
        else {        
          if (new File(path).exists() && !DELDIR) {
            FileUtils.deleteDirectory(new File(path)); 
            DELDIR = true
          }
        }
        
        if (DEBUG) {
          rdd.coalesce(1).saveAsTextFile(path)
        }
        else {
          if(!rdd.isEmpty) {
              rdd.saveAsTextFile(path)
          }
        }
      }
      catch {
        case NonFatal(t) => println("[ERROR] Could not output the result. " + t.toString() + "\n")
      }
  }  
}
