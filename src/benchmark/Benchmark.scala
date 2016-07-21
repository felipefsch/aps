package benchmark

import algorithms._
import utils._
import java.io._
import scala.util.control.NonFatal

/**
 * Benchmarking tool for extracting execution time of APSS implementations
 */
object Benchmark {
  
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
    val writeAll = false
    val nExecs = 1
    
    val INIT = true
    val BRUTEFORCE = true
    val ELEMENTSPLIT = true
    val INVIDX = true
    val INVIDXPRE = true
    val INVIDXFETCH = true
    val INVIDXPREFETCH = true

    // Initialize spark and so on to do not influence in the final execution time
    if (INIT) {
      try {
        algorithms.Init.main(Array())        
      } catch {
        case e:
          Exception => println(e.toString() + "\n\n")
      }
    }           
        
    var configPath = "config/"
    var configs = Array("config0", "config1",
        "config2_0", "config2_1", "config2_2", "config2_3", "config2_4", "config2_5", 
        "config3", "config4", "config5")     
    
    for (config <- configs) {  
      
      val file = new File("/home/schmidt/Desktop/benchmarks/benchmark_" + config + ".txt")
      val fw = new FileWriter(file, true)
      val bw = new BufferedWriter(fw)

      /*var arg0 = Array(
          "--config", configPath + config + ".xml", 
          "--count", "false",
          "--k", "10",
          "--n", "1000", 
          "--debug", "false",           
          "--nElements", "100")
      var arg1 = Array(
          "--config", configPath + config + ".xml", 
          "--count", "false", 
          "--k", "10",
          "--n", "1000", 
          "--debug", "false",          
          "--nElements", "2000")*/
          
      var arg2 = Array(
          "--config", configPath + config + ".xml", 
          "--count", "false", 
          "--k", "10",
          "--n", "5000", 
          "--debug", "false",
          "--createData", "true",  
          "--output", "/home/schmidt/Desktop/benchmarks/results/",
          "--nElements", "100")
      var arg3 = Array(
          "--config", configPath + config + ".xml", 
          "--count", "false", 
          "--k", "10",
          "--n", "5000", 
          "--debug", "false",
          "--createData", "true", 
          "--output", "/home/schmidt/Desktop/benchmarks/results/",          
          "--nElements", "10000")    
          
      /*var arg4 = Array(
          "--config", "config/config3.xml", 
          "--count", "false", 
          "--n", "10000", 
          "--debug", "false",
          "--createData", "true",          
          "--nElements", "100")
      var arg5 = Array(
          "--config", "config/config3.xml", 
          "--count", "false", 
          "--n", "10000", 
          "--debug", "false",
          "--createData", "true",          
          "--nElements", "100")*/
      
      var arguments = Array(/*arg0, arg1,*/ arg2, arg3)//, arg4, arg5)
      
      for (arg <- arguments) { 
        
        Args.parse(arg)
        
        bw.append("\n\n###############################################\n")
        bw.append("# Benchmarking args: " + arg.mkString(" ") + "\n")        
        bw.append("# Benchmarking config: " + config + "\n")
        bw.append("###############################################\n\n")
        bw.flush()
      
        if (Args.CREATEDATA) {
          try {          
            benchmark.SyntheticDataSet.main(arg)
          } catch {
            case e:
              Exception => bw.append(e.toString() + "\n\n")
          }  
        }
        
        bw.flush()
        
        if (ELEMENTSPLIT) {
          bw.append("###Element Split:\n")
          try {
            execTimeAvg(algorithms.ElementSplit.main(arg), nExecs, bw, writeAll)
          } catch {
            case e:
              Exception => bw.append(e.toString() + "\n\n")
          }
        }     
        
        bw.flush()        
        
        if (BRUTEFORCE) {
          bw.append("###Brute Force:\n")
          try {
            execTimeAvg(algorithms.BruteForce.main(arg), nExecs, bw, writeAll)
          } catch {
            case e:
              Exception => bw.append(e.toString() + "\n\n")
          }
        }
        
        bw.flush()
        
        if (INVIDXPREFETCH) {
          bw.append("###Inverted Index Prefix Filtering Fetching IDs:\n")
          try {
            execTimeAvg(algorithms.InvIdxFetchPreFilt.main(arg), nExecs, bw, writeAll)
          } catch {
            case e:
              Exception => bw.append(e.toString() + "\n\n")
          }  
        }      
        
        bw.flush()
        
        if (INVIDXPRE) {
          bw.append("###Inverted Index Prefix Filtering:\n")
          try {
            execTimeAvg(algorithms.InvIdxPreFilt.main(arg), nExecs, bw, writeAll)
          } catch {
            case e:
              Exception => bw.append(e.toString() + "\n\n")
          }     
        }       
        
        bw.flush()        
        
        if (INVIDXFETCH) {
          bw.append("###Inverted Index Fetching IDs:\n")
          try {
            execTimeAvg(algorithms.InvIdxFetch.main(arg), nExecs, bw, writeAll)
          } catch {
            case e:
              Exception => bw.append(e.toString() + "\n\n")
          }
        }
        
        bw.flush()        
        
        if (INVIDX) {
          bw.append("###Inverted Index:\n")
          try {
            execTimeAvg(algorithms.InvIdx.main(arg), nExecs, bw, writeAll)
          } catch {
            case e:
              Exception => bw.append(e.toString() + "\n\n")
          }
        }   
        
        bw.flush()       
        
      }
      bw.close()      
    }
  }
}