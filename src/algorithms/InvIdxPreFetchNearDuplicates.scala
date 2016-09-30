package algorithms

import utils._
import org.apache.spark.rdd.EmptyRDD

object InvIdxPreFetchNearDuplicates {
  
  def main(args: Array[String]): Unit = {
    val sc = Config.getSparkContext(args)
 
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetchPreFilt_c"

    try {  
      // Load also sets ranking size k
      var ranksArray =  Load.loadData(input, sc, Args.partitions)

      var duplicates = sc.emptyRDD[((String, String), Long)]      
      if (Args.GROUPDUPLICATES) {
        ranksArray = Duplicates.groupDuplicates(ranksArray)
        duplicates = Duplicates.getDuplicates(ranksArray)        
      }

      var nearDuplicates = InvIdxPreFetch.run(ranksArray, Args.threshold_c)                                                                                      
      var ranksNearDuplicates = NearDuplicates.groupNearDuplicates(nearDuplicates.map(x => x._1), ranksArray)
      
      var similarRanks = InvIdxPreFetch.run(ranksNearDuplicates, Args.threshold + Args.threshold_c)
      similarRanks = NearDuplicates.filterFalseCandidates(similarRanks)
      similarRanks = NearDuplicates.expandNearDuplicates(similarRanks, ranksArray)
      
      // Add near duplicates to result set
      similarRanks = similarRanks.union(nearDuplicates)
            
      var rddUnion = similarRanks.union(duplicates)
      similarRanks = Duplicates.expandDuplicates(rddUnion)
      
      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS)        
 
    } finally {
      Config.closeSparkContext(sc)
    }
  }  
}