package benchmark

import java.io._
import utils.Args

object Profiling {
  var bw = Benchmark.getWriter()
  
  def stageTime( stage: String, begin: Long, end: Long) {    
    if (Args.PROFILING) {      
      var execTime = end - begin      
      bw.append("[PROFILING] %20d".format(execTime) + " ns: " + stage + "\n")
      bw.flush()
    }
  }  
}