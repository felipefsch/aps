package algorithms

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.xml.XML
import utils._

/**
 * Dummy initialization for more precise benchmarking
 * when running several approaches one after the other
 */
object Init {
  
  // Initialize context and so on to improve benchmark precision
  def main(args: Array[String]): Unit = {  
    // Load config from XML config file
    val configXml = XML.loadFile("config/config1.xml")
    
    var threshold = (((configXml \\ "config") \\ "threshold").text).toDouble
    var input = ((configXml \\ "config") \\ "input").text    
    var output = (((configXml \\ "config") \\ "bruteForce") \\ "output").text
    var master = ((configXml \\ "config") \\ "masterIp").text
    
    val conf = new SparkConf().setMaster(master).setAppName("bruteForce").set("spark.driver.allowMultipleContexts", "true")
      
    val sc = new SparkContext(conf)
    
    sc.stop()
  }
}