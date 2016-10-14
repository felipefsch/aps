package algorithms

import utils._

import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner

object MetricSpace {
  
  /**
   * Input:
   * -in: ([MedoidID, Distance], (ID, [Ranks]*))
   * -radiis: [MedoidID, radii]*
   * -threshold: threshold
   * 
   * Output:
   * -[MedoidID, (String, [Ranks]*)]*
   * 
   * For each medoid, check if Ranking distance to medoid <= radii + threshold
   * If yes, create Tuple with medoid as Key and Ranking as value.
   * 
   * Medoid ID used to partitioning data afterwards
   */
  def entriesToPartition(in: (Array[(String, Long)], (String, Array[String])),
                         radiis: Array[(String, Long)],
                         threshold: Long)
  : List[(String, (String, Array[String]))] = {
    var medoidsDistances = in._1.sortBy(_._1)
    var ranking = in._2
    
    var medoids = List[String]()
    
    // Both arrays are ordered
    for (i <- 1 until medoidsDistances.length) {
      // Check if distance to medoid <= radii + threshold
      if (medoidsDistances(i)._2 <= (radiis(i)._2 + threshold)) {
        medoids = medoidsDistances(i)._1 :: medoids
      }
    }
    
    return medoids.map { x => (x, ranking) }
  }
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    
    // Variables not modifiable. Important when running on a cluster
    // so that all nodes have the correct values
    val output = Args.output + "MetricSpace"
    val masterIp = Args.masterIp
    val threshold = Args.threshold
    val normThreshold = Args.normThreshold
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
    val medoidsMultiplier = Args.medoidsmultiplier
    
    val sc = Config.getSparkContext(masterIp)
    
    try {
      // Load also sets ranking size k
      var ranksArray = Load.loadData(input, sc, partitions, k, n)//.map(x => (x._1, x._2.mkString(":")))
      
      // Take sample and remove element from RDD
      val seed = scala.util.Random.nextInt()
      // TODO ensure we take distinct ones and that they have certain distance between each other??
      val medoids = ranksArray.takeSample(true, if (partitions > 0) {partitions * medoidsMultiplier} else 1, seed)
      val rddMedoids = sc.parallelize(medoids, partitions)

      // Ensure distance between medoids??
      
      // Broadcast medoids
      val broadcastMedoid = sc.broadcast(medoids) 

      // Get distances of entries to medoids. Array with distances is ordered (first is the partition to go to)
      val medoidOrderedDistances = ranksArray.map(x => (Footrule.getMedoidDistances(x, broadcastMedoid.value), x))

      
      // Get radii of each medoid. As there are only few, it's ok to transform them into array
      val medoidsRadii = medoidOrderedDistances.map(x => x._1(0))
                                               .reduceByKey(math.max(_, _))
                                               .collect()
                                               .sortBy(_._1)
      val broadcastRadii = sc.broadcast(medoidsRadii)
      
      // Inner (dist <= radii) and outer points to correct partition (radii < dist <= radii + t)
      val partitionedEntries = medoidOrderedDistances.flatMap(x => entriesToPartition(x, broadcastRadii.value, threshold))
                                                     .partitionBy(new HashPartitioner(partitions))                                                     

      // Compute distances only inside the partition
      val candidates = partitionedEntries.join(partitionedEntries).filter(f => f._2._1._1 < f._2._2._1).map(x => x._2)
      
      val similarRanks = candidates.map(x => Footrule.onLeftIdIndexedArray(x)).filter(f => f._2 <= threshold)

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