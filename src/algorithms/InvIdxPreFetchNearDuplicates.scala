package algorithms

import utils._

object InvIdxPreFetchNearDuplicates {
  
  def main(args: Array[String]): Unit = {
    val sc = Config.getSparkContext(args)
 
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetchPreFilt_c"

    try {  
      // Load also sets ranking size k
      var ranksArray =  Load.loadData(input, sc, Args.partitions)             
           
      var nearDuplicates = InvIdxPreFetch.run(ranksArray, Args.threshold_c)                                           
                                         
      Store.rdd(output + "_duplicates", nearDuplicates, Args.COUNT, Args.STORERESULTS)                                           
      
      var ranksWithDuplicates = NearDuplicates.getNearDuplicates(nearDuplicates.map(x => x._1), ranksArray)
      
      var similarRanks = InvIdxPreFetch.run(ranksWithDuplicates, Args.threshold + Args.threshold_c)
      
      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)
      

    } finally {
      sc.stop()
    }
  }  
}