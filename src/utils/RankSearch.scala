package utils

import scala.xml.XML
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils._

object RankSearch {

  /**
   * Load ranks and retrieve elements of given rank id
   */
  def getRank(rankId: Long, inputPath: String, sc: SparkContext)
  : (Long, Array[Long]) = {
    val ranks = Load.spaceSeparated(inputPath, sc)

    // Filter will give an Array, but as we have unique Id,
    // the first element is the one we are searching for
    val rank = ranks.filter(x => x._2 == rankId).first()
    
    return rank
  }
  
  /**
   * Retrieve rank elements given a rank id for an already loaded input
   */  
  def getRank(rankId: Long, ranks: RDD[(Long, Array[Long])])
  : (Long, Array[Long]) = {
    // Filter will give an Array, but as we have unique Id,
    // the first element is the one we are searching for
    val rank = ranks.filter(x => x._2 == rankId).first()
    
    return rank
  }  
  

  def getRanksTuple(rankIds: (Long, Long), inputPath: String, sc: SparkContext)
  : ((Long, Array[(Long)]), (Long, Array[(Long)])) = {
    val ranks = Load.spaceSeparated(inputPath, sc)
    
    val firstRank = getRank(rankIds._1, ranks)
    val secondRank = getRank(rankIds._2, ranks)
    
    return (firstRank, secondRank)
  }
  
  /**
   * Load ranks and retrieve elements of given rank id tuple
   */
  def getRanksTuple(rankIds: (Long, Long), ranks: RDD[(Long, Array[Long])])
  : ((Long, Array[Long]), (Long, Array[Long])) = {
    //val ranks = InputOutput.loadFirstElementAsId(inputPath, sc)
    
    val firstRank = getRank(rankIds._1, ranks)
    val secondRank = getRank(rankIds._2, ranks)
    
    return (firstRank, secondRank)
  }
}