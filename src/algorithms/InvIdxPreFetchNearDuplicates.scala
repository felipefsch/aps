package algorithms

import utils._
import org.apache.spark.rdd.EmptyRDD

object InvIdxPreFetchNearDuplicates {
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    val sc = Config.getSparkContext(Args.masterIp)
 
    var normThreshold = Args.normThreshold
    var input = Args.input    
    var output = Args.output + "InvIdxFetchPreFilt_c"

    try {  
      // Load also sets ranking size k
      var ranksArray = Load.loadData(input, sc, Args.partitions, Args.k, Args.n)

      var duplicates : org.apache.spark.rdd.RDD[((String, String), Long)] = sc.emptyRDD      
      if (Args.GROUPDUPLICATES) {
        ranksArray = Duplicates.groupDuplicates(ranksArray)
        duplicates = Duplicates.getDuplicates(ranksArray)        
      }

      var nearDuplicates = InvIdxPreFetch.run(ranksArray, Args.threshold_c)                                                                                      
      var ranksNearDuplicates = NearDuplicates.groupNearDuplicates(nearDuplicates.map(x => x._1), ranksArray)
      
      var similarRanks = InvIdxPreFetch.run(ranksNearDuplicates, Args.threshold + Args.threshold_c)
      similarRanks = NearDuplicates.filterFalseCandidates(similarRanks, Args.threshold)
      similarRanks = NearDuplicates.expandNearDuplicates(similarRanks, ranksArray, Args.k, Args.threshold, Args.normThreshold, Args.normThreshold_c)
      
      // Add near duplicates to result set
      similarRanks = similarRanks.union(nearDuplicates)
            
      var rddUnion = similarRanks.union(duplicates)
      similarRanks = Duplicates.expandDuplicates(rddUnion)
      
      Store.rdd(output, similarRanks, Args.COUNT, Args.STORERESULTS, Args.hdfsUri)       
 
    } finally {
      Config.closeSparkContext(sc)
    }
  }  
}