package algorithms

import utils._
import org.apache.spark.rdd.EmptyRDD
import org.apache.log4j._


object ElementSplitNearDuplicates {
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    
    // Variables not modifiable. Important when running on a cluster
    // so that all nodes have the correct values
    val output = Args.output + "ElementSplit_c"
    val masterIp = Args.masterIp
    val threshold = Args.threshold
    val threshold_c = Args.threshold_c
    val normThreshold = Args.normThreshold
    val normThreshold_c = Args.normThreshold_c
    val input = Args.input
    val k = Args.k
    val n = Args.n
    val minOverlap = Args.minOverlap
    val hdfsUri = Args.hdfsUri
    val partitions = Args.partitions
    val COUNT = Args.COUNT
    val DEBUG = Args.DEBUG
    val STORERESULTS = Args.STORERESULTS      
    val GROUPDUPLICATES = Args.GROUPDUPLICATES    
    
    val sc = Config.getSparkContext(masterIp)

    try {  
      // Load also sets ranking size k
      var ranksArray = Load.loadData(input, sc, partitions, k, n)

      var duplicates : org.apache.spark.rdd.RDD[((String, String), Long)] = sc.emptyRDD      
      if (GROUPDUPLICATES) {
        ranksArray = Duplicates.groupDuplicates(ranksArray)
        duplicates = Duplicates.getDuplicates(ranksArray)        
      }

      var nearDuplicates = ElementSplit.run(ranksArray, threshold_c, k, minOverlap)                                                                                      
      var ranksNearDuplicates = NearDuplicates.groupNearDuplicates(nearDuplicates.map(x => x._1), ranksArray)
      
      var similarRanks = ElementSplit.run(ranksNearDuplicates, threshold + threshold_c, k, minOverlap)
      similarRanks = NearDuplicates.filterFalseCandidates(similarRanks, threshold)
      similarRanks = NearDuplicates.expandNearDuplicates(similarRanks, ranksArray, k, threshold, normThreshold, normThreshold_c)
      
      // Add near duplicates to result set
      similarRanks = similarRanks.union(nearDuplicates)
            
      if (GROUPDUPLICATES) {
        var rddUnion = similarRanks.union(duplicates)
        similarRanks = Duplicates.expandDuplicates(rddUnion)
      }
      
      Store.rdd(output, similarRanks, COUNT, STORERESULTS, hdfsUri)       
    } catch {
      case e:
        Exception => val log = LogManager.getRootLogger
        log.error(e.toString())
    } finally {
      Config.closeSparkContext(sc)
    }
  }  
}