package utils

/**
 * This object can be used to find mismatching outputs from different approaches.
 * It can be used for debugging and correcteness check.
 */
object FindMismatch {
  
    def main(args: Array[String]): Unit = {
      
      val sc = Config.getSparkContext("local")
      
      var FILE1 = sc.textFile("output/ElementSplit/part-00000")
      var FILE2 = sc.textFile("output/ElementSplit_c/part-00000")
      
      var aux1 = FILE1.map(x => (x.substring(0, x.indexOf(")") + 1), 1))
      var aux2 = FILE2.map(x => (x.substring(0, x.indexOf(")") + 1), 1))
      var union = aux1.union(aux2)
     
      // Check if there are more than 1 replica of the pairs in the provided files.
      var duplicates1 = aux1.reduceByKey((a, b) => a + b).filter(f => f._2 > 1)
      var duplicates2 = aux2.reduceByKey((a, b) => a + b).filter(f => f._2 > 1)
      Store.rdd("output/duplicated1", duplicates1, true, true, "")
      Store.rdd("output/duplicated2", duplicates2, true, true, "")
      
      // Check if pair appear in only one file and if it appears more
      // than twice considering both files.
      var reduced = union.reduceByKey((a, b) => a + b)      
      var mismatches = reduced.filter(f => f._2 > 2 || f._2 == 1)
      Store.rdd("output/mismatches", mismatches, true, true, "")
    }
}
