package benchmark

import algorithms._
import utils._
import java.io._
import java.util.Calendar
import scala.util.control.NonFatal

/**
 * Benchmarking tool for extracting execution time of APSS implementations
 */
object Benchmark {

  /**
   * Get benchmarking output writer
   */
  def getWriter() : BufferedWriter = {
    val file = new File(Args.benchmarkOutput)
    var bw = new BufferedWriter(new FileWriter(file, true)) 
    return bw
  }
  
  /**
   * Get execution time of block in nanoseconds
   */
  def timeNs[R](block: => R, bw: BufferedWriter): Long = {
    var result = 0.toLong
    try {
      val t0 = System.nanoTime()
      block    // call-by-name
      val t1 = System.nanoTime()
      result = (t1 - t0)
    }
    catch {
      case e: Exception => {
        bw.append("[ERROR] " + e.toString() + "\n")
        bw.flush()        
        
        result = -1                
      }
    }
    return result
  }
  
  /**
   * Input:
   * -block: block code to be executed
   * -nExecs: number of executions
   * -bw: file writer to benchmarking file
   * -writeAll: write all outputs? Otherwise, only AVG
   * 
   * Execute block multiple times and write
   * average execution time on file 
   */
  def execTimeAvg[R](block: => R, nExecs: Int, bw: BufferedWriter, writeAll: Boolean): Unit = {
    try {
      var totalExecTime = 0.toLong
      for (i <- 1 to nExecs) {
        var execTime = timeNs(block,bw)
        totalExecTime += execTime
        if (writeAll)
          bw.append("[BENCHMARK] " + "%20d".format(execTime) + " ns: Execution " + i + "\n")
          bw.flush()
      }
      bw.append("[BENCHMARK] "          
          +  "%20d".format(totalExecTime / nExecs)
          + " ns: AVG Execution time\n\n") 
      bw.flush()
    } catch {
      case e:
        Exception => bw.append(e.toString() + "\n\n")
        bw.flush()
    }    
  }
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)    

    var writeAll = Args.WRITEALL

    // Initialize spark and so on to do not influence in the final execution time
    if (Args.INIT) {
      try {
        algorithms.Init.main(args)        
      } catch {
        case e:
          Exception => println(e.toString() + "\n\n")
      }
    }
  
    if (Args.CREATEDATA) {
      try {          
        benchmark.SyntheticDataSet.main(args)
      } catch {
        case e:
          Exception => println("\n" + e.toString() + "\n")
      }  
    }
    
    if (Args.BENCHMARK) {     
      // Set file writer
      var bw = getWriter()
      var now = Calendar.getInstance()
      var hour = now.get(Calendar.HOUR)
      var minute = now.get(Calendar.MINUTE)
      var day = now.get(Calendar.DATE)
      var month = now.get(Calendar.MONTH) + 1
      bw.append("\n\n###############################################\n")    
      bw.append("# Benchmarking started at " + hour + ":" + minute)
      bw.append(" (" + day + "/" + month + ")\n")
      bw.append("-k: " + Args.k + "\n")      
      bw.append("-n: " + Args.n + "\n")
      bw.append("-threshold: " + Args.normThreshold + "\n")
      bw.append("-threshold_c: " + Args.normThreshold_c + "\n")
      bw.append("-input data: " + Args.input + "\n")
      bw.append("-store final results: " + Args.STORERESULTS + "\n")      
      bw.append("-pre group duplicates: " + Args.PREGROUP + "\n")
      bw.append("-pre group near duplicates: " + Args.NEARDUPLICATES + "\n")                 
      bw.append("###############################################\n\n")
      bw.flush()      
      
      if (Args.ELEMENTSPLIT) {
        bw.append("###Element Split:\n")
        bw.flush()
        execTimeAvg(algorithms.ElementSplit.main(args), Args.nExecs, bw, writeAll)
      }     
      
      if (Args.BRUTEFORCE) {
        bw.append("###Brute Force:\n")
        bw.flush()
        execTimeAvg(algorithms.BruteForce.main(args), Args.nExecs, bw, writeAll)
      }
      
      if (Args.INVIDXPREFETCH) {
        bw.append("###Inverted Index Prefix Filtering Fetching IDs:\n")
        bw.flush()
        execTimeAvg(algorithms.InvIdxPreFetch.main(args), Args.nExecs, bw, writeAll)
      }      
      
      if (Args.INVIDXPRE) {
        bw.append("###Inverted Index Prefix Filtering:\n")
        bw.flush()
        execTimeAvg(algorithms.InvIdxPreFilt.main(args), Args.nExecs, bw, writeAll)
      }               
      
      if (Args.INVIDXFETCH) {
        bw.append("###Inverted Index Fetching IDs:\n")
        bw.flush()
        execTimeAvg(algorithms.InvIdxFetch.main(args), Args.nExecs, bw, writeAll)
      }      
      
      if (Args.INVIDX) {
        bw.append("###Inverted Index:\n")
        bw.flush()
        execTimeAvg(algorithms.InvIdx.main(args), Args.nExecs, bw, writeAll)
      }
      
      if (Args.INVIDXPREFETCH_C) {
        bw.append("###Inverted Index Prefix Filtering Fetching IDs with near duplicates:\n")
        bw.flush()
        execTimeAvg(algorithms.InvIdxPreFetchNearDuplicates.main(args), Args.nExecs, bw, writeAll)
      }      
      
      now = Calendar.getInstance()
      hour = now.get(Calendar.HOUR)
      minute = now.get(Calendar.MINUTE)
      day = now.get(Calendar.DATE)
      month = now.get(Calendar.MONTH) + 1
      bw.append("\n\n###############################################\n")    
      bw.append("# Endet at " + hour + ":" + minute)
      bw.append(" (" + day + "/" + month + ")\n")
      bw.append("###############################################\n")
      bw.flush()            
      
      bw.close()
    }
  }
}