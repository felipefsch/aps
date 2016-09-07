package utils

import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Given an input with space separated elements, with one rank per line,
 * create unique IDs for each rank and output it as:
 * 
 * RankId e1 e2 e3 e4 ... ek
 * 
 * All space separated elements for easiness of computation and storage
 */
object UniqueIdOnRanks {
  
  /**
   * Add unique ID on ranking files
   * 
   * The best way to do it is to use the spark-shell and copy the commands
   * after SparkContext creation. It will generate a folder in the same input path
   * with .indexed in the end with many part-0000x inside. To merge them into a single
   * file you can run $cat part-00000 part-00001 ... > out.txt
   * which will merge into out.txt file.
   */
  def idOnRealDataset(path: String)
  {
    val sc = Config.getSparkContext(Array[String]())
    val file = sc.textFile(path)
    val lines = file.map(x => x.split("\n"))
    val indexedLines = lines.zipWithUniqueId()
    val swapedOrder = indexedLines.map(x => (x._2, x._1.mkString("")))
    swapedOrder.saveAsTextFile(path + ".indexed")
    //Store.rdd(path + ".indexed", swapedOrder, false, true)
  }
  
  /**
   * Transform elements array into string for better output representation
   * e.g. not including string "List(" in the output
   */
  def arrayIdToString ( ar: Array[String], id: Long) : String = {
    var auxStr = id.toString() + " "
    
    for (i <- 0 until ar.size) {
      auxStr = auxStr + ar(i).toString() + " "
    }    
    
    return auxStr    
  }
  
  def main(args: Array[String]): Unit = {
    
    val configXml = XML.loadFile("config/config.xml")
    var input = ((configXml \\ "config") \\ "ranks").text
    var output = ((configXml \\ "config") \\ "ranksUniqueId").text
    var master = ((configXml \\ "config") \\ "masterIp").text
    
    val conf = new SparkConf().setMaster(master).setAppName("UniqueIdOnRanks")
    
    val sc = new SparkContext(conf)
    
    // File reading
    val ranks = sc.textFile(input)
    
    val ranksArray = ranks.map(b => b.split(" "))
    
    // Create unique IDs to ranks
    val ranksArrayUniqueId = ranksArray.zipWithUniqueId()
    
    val ranksListUniqueId = ranksArrayUniqueId.map(x => arrayIdToString(x._1, x._2))
    
    ranksListUniqueId.saveAsTextFile(output)
    
    sc.stop()
  }
}