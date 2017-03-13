package utils


object FindMismatch {
  
    def main(args: Array[String]): Unit = {
      
      val sc = Config.getSparkContext("local")
      
      var FILE1 = sc.textFile("output/InvIdxFetchPreFilt/part-00000")
      var FILE2 = sc.textFile("output/InvIdxFetchPreFilt_c/part-00000")
      
      var aux1 = FILE1.map(x => (x.substring(0, x.indexOf(")") + 1), 1))
      var aux2 = FILE2.map(x => (x.substring(0, x.indexOf(")") + 1), 1))
      var union = aux1.union(aux2)
     
      var reduced = union.reduceByKey((a, b) => a + b)
      
      var mismatches = reduced.filter(f => f._2 > 2 || f._2 == 1)
      
      Store.rdd("output/mismatches", mismatches, true, true, "")
    }
}
