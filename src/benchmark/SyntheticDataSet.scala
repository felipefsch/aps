package benchmark

import scala.xml.XML
import java.io._
import utils.Footrule
import utils.Args

object SyntheticDataSet {
  
  private var increaseSequential = true

  /**
   * Input:
   * -sequential: current sequential number
   * -min: minimum sequential number
   * -max: maximum sequential number
   *  
   * Change sequential element to be only within specified range
   */
  private def changeSequential(sequential: Long, min: Long, max: Long) : Long = {
    
    if (sequential == min) {
      increaseSequential = true
    }
    if (sequential == max) {
      increaseSequential = false
    }
    
    if (increaseSequential)
      return sequential + 1
    else
      return sequential - 1
  }
  
  /**
   * Input:
   * -k: size of ranking
   * -pool: pool of common elements
   * -minPoolElements: minimum amount of elements from pool to be selected
   * -maxPoolElements: maximum amount of elements from pool to be selected
   * -nextSequential: next sequential number used to non-shared elements
   * -id: ranking ID
   * -bw: file writer to write ranking
   * 
   * Output:
   * -next sequential number to be used
   * 
   * Create one ranking vector
   */
  def createRanking(k:Int,
      pool: Array[Long],
      minPoolElements: Long,
      maxPoolElements: Long,
      currentSequential: Long,
      minSequential: Long,
      maxSequential: Long,
      id: Long,
      bw: BufferedWriter)
  : Long = {
    val rand = scala.util.Random
    val itemsFromPool = rand.nextInt((maxPoolElements - minPoolElements + 1).toInt) + minPoolElements
    
    if (Args.DEBUG) {
      println("### Creating Ranking ID " + id)      
      println("k: " + k)
      println("Pool: " + pool.mkString(" "))
      println("Pool size: " + pool.size)  
      println("Items from pool: " + itemsFromPool)
      println("Min pool elements: " + minPoolElements)
      println("Max pool elements: " + maxPoolElements)    
      println("Next sequential: " + minSequential)
    }
    
    // Start always from first element, otherwise no overlapping rankings
    var poolIndex = 0//rand.nextInt(pool.size - itemsFromPool.toInt + 1)
    
    if (Args.DEBUG) {
      println("Pool starting index: " + poolIndex)
      println("Max start index: " + (pool.size - itemsFromPool.toInt))
    }    
        
    // Initialize array with zeros
    val ranking = Array.ofDim[Long](k)

    var sequential = currentSequential    
    var index = 0
    
    // Elements from pool
    for (i <- 0 until itemsFromPool.toInt) { 
      if (Args.DEBUG) {
        println("From pool on position: " + i + " value " + pool(poolIndex))
      }
 
      ranking(i) = pool(poolIndex)
      poolIndex += 1
    }
    
    // Non-overlapping elements
    for (i <- itemsFromPool.toInt until k) {
      if (Args.DEBUG) {
        println("Not from pool on position: " + i + " value " + sequential)
      }
      
      ranking(i) = sequential
      
      sequential = changeSequential(sequential, minSequential, maxSequential)
    }
    
    bw.append(id + " " + ranking.mkString(" "))
    bw.newLine()
    
    return sequential
  }
  
  /**
   * Input:
   * -k: size of ranking
   * -n: number of rankings
   * -output: path where to store data set
   * -threshold: normalized desired Footrule threshold
   * required for computing overlaps
   * -selectivity: percentage of similar pairs
   * -nPools: number of pools to create rankings intersection.
   * It implies partitioning into subsets
   * -poolIntersection: percentage of intersection between
   * neighbor pools
   * 
   *    
   */
  def create(k: Int,
      n: Int,
      output: String,
      threshold: Double,
      selectivity: Double,
      nPools: Int,
      poolIntersection: Double,
      distinctElements: Long)
  : Unit = {

    val minOverlap = utils.Footrule.getMinOverlap(k, threshold)
    val combinations = n * (n - 1)
    val maxDist = Footrule.denormalizeThreshold(k, threshold)
    // Maximum number of non common elements in the end of the ranking
    // necessary to ensure selectivity for the given threshold
    var maxNonCommonElements = 0
    
    var auxDist = 0
    // Elements on the end of the ranking contribute to Footrule
    // distance as a progression 1, 2, 3... should not exceed maxDist
    for ( i <- 1 to k) {
      auxDist += i
      if (auxDist >= (maxDist / 2).toInt && maxNonCommonElements == 0) {
        maxNonCommonElements = i - 1
      }
    }
    
    // Minimum elements from pool that must be shared in order
    // to achieve minimum threshold
    var minCommonElements = k - maxNonCommonElements

    if (Args.DEBUG) {
      println("###### CREATING NEW SYNTHETIC DATA SET ######")
      println("n: " + n)
      println("k: " + k)
      println("Normalized threshold: " + threshold)
      println("Maximum footrule distance: " + maxDist)
      println("Minimum common elements for candidates: " + minCommonElements)      
      println("Maximum non common elements for candidates: " + maxNonCommonElements) 
      println("Number of pools: " + nPools)
      println("Intersection between pools: " + poolIntersection)
      println("Selectivity: " + selectivity)
    }
    
    val file = new File(output)
    
    if (file.exists() && Args.CREATEDATA) {
      file.delete()
      
      if (Args.DEBUG)
        println("Delting existing input file " + file)
    }
    
    val bw = new BufferedWriter(new FileWriter(file))

    ////////// Pool of common elements from PRUNED pairs (don't reach minimum overlap)
    val prunedPoolSize = minOverlap - 1  
    val lastPool1Pruned = minOverlap - 1 // Last element from first pool of pruned pairs 
    val nIntersecPruned = math.max(math.round(prunedPoolSize * poolIntersection), 1)
    val poolPruned = (1.toLong to ((lastPool1Pruned) + ((nPools - 1) * (prunedPoolSize - nIntersecPruned)))).toArray
    val lastPruned = poolPruned.last
    var prunedFirstIndex = 0
    var prunedLastIndex = minOverlap - 1

    ////////// Pool of common elements from CANDIDATE pairs
    val candidatesPoolSize = k 
    val lastPool1Candidate = lastPruned + k
    val nIntersecCandidates = math.max(math.round(candidatesPoolSize * poolIntersection), 1)
    val poolCandidates = ((lastPruned + 1) to ((lastPool1Candidate) + ((nPools - 1) * (candidatesPoolSize - nIntersecCandidates)))).toArray
    var candidateFirstIndex = 0
    var candidateLastIndex = k
    
    
    // Exact number of rankings created might differ from input due to rounding
    var partitionSize = math.max((math.abs(n / nPools)), 1)
    
    // For each partition, how many rankings to be candidates and how many not
    var nSelected = (partitionSize * selectivity).toInt
    var nNotSelected = (partitionSize * (1 - selectivity)).toInt
    
    // Non overlapping elements start from end of pools plus
    // 10 for differentiation
    var minSequential = math.max(poolPruned.max, poolCandidates.max) + 1
    var currentSequential = minSequential
    var maxSequential = distinctElements
    
    var id = 0
    
    // Each partition uses one pool and the partitions are uniformly distributed
    for (p <- 0 until nPools) {
      
      if (Args.DEBUG) {
        println("\n\n#####Using pool " + p + "#####")
        println("Pruned pool position slice [" + prunedFirstIndex + "," + prunedLastIndex + ")")
        println("Pruned pool elements intersection: " + nIntersecPruned)
        println("Pruned pool size: " + poolPruned.size)        
        println("Pruned pool size per partition: " + prunedPoolSize)
        println("Candidates pool position slice [" + candidateFirstIndex + "," + candidateLastIndex + ")")
        println("Candidates pool elements intersection: " + nIntersecCandidates)
        println("Candidates pool size: " + poolCandidates.size)        
        println("Candidates pool size per partition: " + candidatesPoolSize)
        println("Minimum ranking overlap: " + minOverlap)
        println("Pool pruned elements: " + poolPruned.mkString(" "))
        println("Pool candidate pairs: " + poolCandidates.mkString(" "))
        println("Partitions size: " + partitionSize)
        println("Initial sequential number: " + minSequential)
        println("Rankings that will be selected: " + nSelected)
        println("Rankings that will not be selected: " + nNotSelected) 
      }

      // Generate pairs that will not be selected in the output
      for (j <- 0 until nNotSelected) {
        var slicedPool = poolPruned.slice(prunedFirstIndex.toInt,prunedLastIndex.toInt)
        currentSequential = createRanking(k, slicedPool, 0, minOverlap - 1, currentSequential, minSequential, maxSequential, id, bw)
        id += 1
      }
      
      // Generate pairs that will be selected in the output
      for (j <- 0 until nSelected) {
        var slicedPool = poolCandidates.slice(candidateFirstIndex.toInt,candidateLastIndex.toInt)
        currentSequential = createRanking(k, slicedPool, minCommonElements, k, currentSequential, minSequential, maxSequential, id, bw)
        id += 1          
      }
      
      // Shifting pool to the next partition by changing its indexes
      prunedFirstIndex = prunedLastIndex.toInt - nIntersecPruned.toInt
      prunedLastIndex += (prunedPoolSize.toInt - nIntersecPruned.toInt)
      candidateFirstIndex = candidateLastIndex.toInt - nIntersecCandidates.toInt
      candidateLastIndex += (candidatesPoolSize.toInt - nIntersecCandidates.toInt)

    }
    bw.close()
  }
  
  def main(args: Array[String]) {
    Args.parse(args)

    var k = Args.k
    var n = Args.n
    var distinctElements = Args.distinctElements
    var output = Args.datasetOutput
    var normThreshold = Args.normThreshold
    var nPools = Args.nPools
    var selectivity = Args.selectivity
    var poolIntersection = Args.poolIntersection

    // Maximum number of distinct elements if -1 used
    if (distinctElements < 0)
      distinctElements = n * k
    
    if (Args.DEBUG)
      println("CREATING DATA SET")
    
    create(k,
                      n,
                      output,
                      normThreshold,
                      selectivity,
                      nPools,
                      poolIntersection,
                      distinctElements)

    if (Args.DEBUG)
      println("DONE!")
  }
  
}