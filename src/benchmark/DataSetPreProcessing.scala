package benchmark

import utils._

import org.apache.commons.lang.StringUtils

/**
 * Pre-process data set to output exact number of entries with minimum ranking size 
 */
object DataSetPreProcessing {
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    
    val k = Args.k
    val n = Args.n    
    val masterIp = Args.masterIp
    val input = Args.input
    val output = Args.datasetOutput
    val partitions = 1 //Args.partitions
    
    val sc = Config.getSparkContext(masterIp)
    val file = sc.textFile(input, partitions)
    val filtered = file.filter(
      x => StringUtils.countMatches(
            x.substring(x.lastIndexOf("\t") + 1, x.length() - 1), ":"
          ) > k - 1
    )

    val filterAmount = filtered.take(n)
    
    val filteredInput = sc.parallelize(filterAmount)
                          .repartition(partitions)
                          
    Store.rdd(output, filteredInput, false, true, "")
    
    Config.closeSparkContext(sc)
  }
}