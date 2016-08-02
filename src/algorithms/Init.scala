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
    Args.parse(args)

    var master = Args.masterIp
    
    if (Args.DEBUG)
      println("[DEBUG] Initializing Spark context...")
    
    val conf = new SparkConf().setMaster(master)
                    .setAppName("init")
                    .set("spark.driver.allowMultipleContexts", "true")
      
    val sc = new SparkContext(conf)
    
    sc.stop()
    
    if (Args.DEBUG)
      println("[DEBUG] Spark context initialized!")
  }
}