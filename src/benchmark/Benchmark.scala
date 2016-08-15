package benchmark

import algorithms._
import utils._
import java.io._
import scala.util.control.NonFatal

/**
 * Benchmarking tool for extracting execution time of APSS implementations
 */
object Benchmark {
  
  var bw = new BufferedWriter(new FileWriter(new File("")))
  
  def stageTime( stage: String, begin: Long, end: Long) {
    var execTime = end - begin
    if (Args.PROFILING)
      bw.append("Execution of " + stage + ": " + execTime.toString() + "ns")
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
        bw.append("\nException: " + e.toString() + "\n\n")
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
    var totalExecTime = 0.toLong
    for (i <- 1 to nExecs) {
      var execTime = timeNs(block,bw)
      totalExecTime += execTime
      if (writeAll)
        bw.append("Execution " + i + ": " + execTime + " ns\n")
        bw.flush()
    }
    bw.append("AVG Execution time: " + (totalExecTime / nExecs) + " ns\n\n")        
  }
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
     
    // Set file writer
    val file = new File(Args.benchmarkOutput)
    bw = new BufferedWriter(new FileWriter(file, true))    

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
      bw.append("\n\n###############################################\n")    
      bw.append("# Benchmarking config: " + Args.benchmarkOutput + "\n")
      bw.append("###############################################\n\n")
      bw.flush()      
      
      if (Args.ELEMENTSPLIT) {
        bw.append("###Element Split:\n")
        try {
          execTimeAvg(algorithms.ElementSplit.main(args), Args.nExecs, bw, writeAll)
        } catch {
          case e:
            Exception => bw.append(e.toString() + "\n\n")
        }
      }     
      
      bw.flush()        
      
      if (Args.BRUTEFORCE) {
        bw.append("###Brute Force:\n")
        try {
          execTimeAvg(algorithms.BruteForce.main(args), Args.nExecs, bw, writeAll)
        } catch {
          case e:
            Exception => bw.append(e.toString() + "\n\n")
        }
      }
      
      bw.flush()
      
      if (Args.INVIDXPREFETCH) {
        bw.append("###Inverted Index Prefix Filtering Fetching IDs:\n")
        try {
          execTimeAvg(algorithms.InvIdxFetchPreFilt.main(args), Args.nExecs, bw, writeAll)
        } catch {
          case e:
            Exception => bw.append(e.toString() + "\n\n")
        }  
      }      
      
      bw.flush()
      
      if (Args.INVIDXPRE) {
        bw.append("###Inverted Index Prefix Filtering:\n")
        try {
          execTimeAvg(algorithms.InvIdxPreFilt.main(args), Args.nExecs, bw, writeAll)
        } catch {
          case e:
            Exception => bw.append(e.toString() + "\n\n")
        }     
      }       
      
      bw.flush()        
      
      if (Args.INVIDXFETCH) {
        bw.append("###Inverted Index Fetching IDs:\n")
        try {
          execTimeAvg(algorithms.InvIdxFetch.main(args), Args.nExecs, bw, writeAll)
        } catch {
          case e:
            Exception => bw.append(e.toString() + "\n\n")
        }
      }
      
      bw.flush()        
      
      if (Args.INVIDX) {
        bw.append("###Inverted Index:\n")
        try {
          execTimeAvg(algorithms.InvIdx.main(args), Args.nExecs, bw, writeAll)
        } catch {
          case e:
            Exception => bw.append(e.toString() + "\n\n")
        }
      }   
      
      bw.flush()
      
      bw.close()
    }
  }
}