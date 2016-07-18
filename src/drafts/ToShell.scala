package drafts

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.nio.file.Files
import java.nio.file.Paths
import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 *  Some draft code that might be useful when testing stuff on spark-shell 
 */
object ToShell {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setMaster("local").setAppName("ToShell")
    
    val sc = new SparkContext(conf)
    
    var input = "/home/schmidt/Dropbox/Master/Thesis/workspace/APSS-on-Ranks/input/verySmallRanksId/part-00000"
    
    val ranks = sc.textFile(input)
  }
}