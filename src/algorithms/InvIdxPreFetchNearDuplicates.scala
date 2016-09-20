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
      
      if (Args.GROUPDUPLICATES) {
        ranksArray = Duplicates.groupDuplicates(ranksArray)
      }
        
      if (Args.GROUPNEARDUPLICATES) {
        var nearDuplicates = InvIdxPreFetch.run(ranksArray, Args.threshold_c)                                           
        Store.rdd(output + "_duplicates", nearDuplicates, Args.COUNT, Args.STORERESULTS)                                           
        ranksArray = NearDuplicates.groupNearDuplicates(nearDuplicates.map(x => x._1), ranksArray)
        //Store.rdd(output + "_lala", ranksArray, Args.COUNT, Args.STORERESULTS)
        
        var similarRanks = InvIdxPreFetch.run(ranksArray, Args.threshold + Args.threshold_c)    
        Store.rdd(output + "_notexpanded", similarRanks, Args.COUNT, Args.STORERESULTS) 
        if (Args.GROUPDUPLICATES) {
          similarRanks = Duplicates.expandDuplicates(similarRanks)
        }
        
        Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)        
      }      
      else {
        var similarRanks = InvIdxPreFetch.run(ranksArray, Args.threshold)

        if (Args.GROUPDUPLICATES) {
          similarRanks = Duplicates.expandDuplicates(similarRanks)
        }
        
        Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)          
      }
 
    } finally {
      sc.stop()
    }
  }  
}