package drafts

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.xml.XML
import utils._

object ElementSplit {
  
    // TODO should test eveything and check usability. Do filter and inverted index first
  
    def main(args: Array[String]): Unit = {
      // Load config from XML config file
      val configXml = XML.loadFile("config/config.xml")
      
      var threshold = (((configXml \\ "config") \\ "threshold").text).toInt
      var input = ((configXml \\ "config") \\ "ranks").text
      var output = ((configXml \\ "config") \\ "bruteForceOutput").text
      var master = ((configXml \\ "config") \\ "masterIp").text
      
      val conf = new SparkConf().setMaster(master).setAppName("bruteForceFootrule")
      
      val sc = new SparkContext(conf)
      
      // File reading
      val ranks = sc.textFile(input)
      
      // Partition ranks
      var ranksInt = ranks.map(b => b.split(", ").map(_.toInt))
      
      // Add rankId to each array
      val ranksIntIndex = ranksInt.zipWithUniqueId()
      
      // Create Array(rank, (rankId, rankPos))
      val rankTupleIdPos = ranksIntIndex.flatMap(x => x._1.zipWithIndex.map(y => Tuple2(y._1, Tuple2(x._2, y._2))))
      
      // Grouped Ranks as (rank, (rankId, rankPos)*)
      val groupedRanks = rankTupleIdPos.groupBy(tup => tup._1)
      
      // Transform CompactBuffer into Array for easier iteration
      val groupedRanksArray = groupedRanks.map(x => x._2.toArray)
      
      // Partial Footrule Distances with some ((0,0),0) vector which must to be filtered
      val partialDistances = groupedRanksArray.map(x => { for (i <- 0 until x.size; j <- 0 until x.size) yield { if (x(i)._2._1 < x(j)._2._1) ((x(i)._2._1.toLong, x(j)._2._1.toLong), math.abs(x(i)._2._2 - x(j)._2._2)) else ((0.toLong,0.toLong), 0) } } )
      
      // Transforming collection into array for easier iteration
      val partialDistancesArray = partialDistances.flatMap(x => x).groupBy(tup => tup._1).map(x => (x._1, x._2.toArray))
      
      // Partial distance without considering ranks not existent in the rank list
      val partialFootrule = partialDistancesArray.map(x => x._2.reduce((a, b) => (a._1, a._2 + b._2)))
    }
}