package utils

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Config {
  
  /**
   * Get spark context based on input data. It also parses
   * the used arguments
   */
  def getSparkContext(args: Array[String]) 
  : SparkContext = {
    Args.parse(args)
    
    val master = Args.masterIp
    
    val conf = new SparkConf()
            .setMaster(master)
            .setAppName("APS")
            //.set("spark.driver.allowMultipleContexts", "true")
            //.set("spark.executor.cores", Args.cores)
            //.set("spark.executor.memory", "8G")
            //.set("spark.executor.instances", Args.executors)
            //.set("spark.dynamicAllocation.enabled", Args.dynamicAllocation)
    
    val sc = SparkContext.getOrCreate(conf)
    //sc.setLogLevel("ERROR")                        
            
    //var t = SparkContext.getOrCreate(conf)
    return sc
  }
  
  def closeSparkContext(sc: SparkContext) {
    if (sc.isLocal) {
      sc.stop()
    }
  }
}