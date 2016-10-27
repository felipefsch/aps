package benchmark

import algorithms._
import utils._
import java.io._
import java.util.Calendar
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

/**
 * Benchmarking tool for extracting execution time of APSS implementations
 */
object Benchmark {

  /**
   * Get benchmarking output writer
   */
  /*def getWriter(filePath: String) : BufferedWriter = {
    val file = new File(filePath)
    var bw = new BufferedWriter(new FileWriter(file, true)) 
    return bw
  }*/
  
  /**
   * Write benchmarking result to either HDFS or local file system, depending
   * input argument
   */
  def getWriter(filePath: String) : Either[BufferedWriter, PrintWriter] = {
    if (filePath.contains("hdfs")) {    
      val conf = new Configuration()
      //conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")
      val hdfsUrl = filePath.substring(0, filePath.lastIndexOf(":"))
      var aux = filePath.substring(hdfsUrl.length(), filePath.length())
      val port = aux.substring(aux.indexOf(":") + 1, aux.indexOf("/"))
      aux = aux.substring(aux.indexOf("/"), aux.length())
      val file = aux
      
      conf.set("fs.defaultFS", hdfsUrl + ":" + port)
      val fs= FileSystem.get(conf)
      val output = fs.create(new Path(file))
      val writer = new PrintWriter(output)
      
      return Right(writer)
    }
    else {
      val file = new File(filePath)
      var bw = new BufferedWriter(new FileWriter(file, true))
      
      return Left(bw)      
    }
  }
  
  /**
   * Close the writer
   */
  def closeWriter(writer: Either[BufferedWriter, PrintWriter]) {
    writer match {
      case Left(b) =>
        b.close()
      case Right(p) =>
        p.close()
    }
  }
  
  /**
   * Write message to benchmarking file
   */
  def write(writer: Either[BufferedWriter, PrintWriter], msg: String) {
    writer match {
      case Left(b) =>
        b.append(msg).flush()
      case Right(p) =>
        p.append(msg).flush()
    }
  }
  
  /**
   * Get execution time of block in nanoseconds
   */
  def timeNs[R](block: => R, w: Either[BufferedWriter, PrintWriter]): Long = {
    var result = 0.toLong
    try {
      val t0 = System.nanoTime()
      block    // call-by-name
      val t1 = System.nanoTime()
      result = (t1 - t0)
    }
    catch {
      case e: Exception => {
        write(w, "[ERROR] " + e.toString() + "\n")        
        
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
  def execTimeAvg[R](block: => R, nExecs: Int, w: Either[BufferedWriter, PrintWriter], writeAll: Boolean): Unit = {
    try {
      var totalExecTime = 0.toLong
      var now = Calendar.getInstance()
      var hour = now.get(Calendar.HOUR)
      var minute = now.get(Calendar.MINUTE)       
      write(w, "[INFO] Start at: " + "%2d".format(hour) + ":" + "%2d".format(minute) + "\n")
      for (i <- 1 to nExecs) {
        var execTime = timeNs(block, w)
        totalExecTime += execTime
        if (writeAll)
          write(w, "[BENCHMARK] " + "%20d".format(execTime) + " ns: Execution " + i + "\n")
      }
      now = Calendar.getInstance()
      hour = now.get(Calendar.HOUR)
      minute = now.get(Calendar.MINUTE)       
      write(w, "[BENCHMARK] "          
          +  "%20d".format(totalExecTime / nExecs)
          + " ns: AVG Execution time\n")
      write(w, "[INFO] End at:   " + "%2d".format(hour) + ":" + "%2d".format(minute) + "\n\n")          
    } catch {
      case e:
        Exception => write(w, e.toString() + "\n\n")
    }
  }
  
  def main(args: Array[String]): Unit = {
    Args.parse(args)
    
    // Spark context to run benchmarks
    //val sc = Config.getSparkContext(Args.masterIp)

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
      var w = getWriter(Args.benchmarkOutput)
      var now = Calendar.getInstance()
      var hour = now.get(Calendar.HOUR)
      var minute = now.get(Calendar.MINUTE)
      var day = now.get(Calendar.DATE)
      var month = now.get(Calendar.MONTH) + 1
      write(w, "\n\n###############################################################################\n")
      write(w, "# Benchmarking started at " + hour + ":" + minute)
      write(w, " (" + day + "/" + month + ")\n")
      write(w, "-k: " + Args.k + "\n")
      write(w, "-n: " + Args.n + "\n")
      write(w, "-count: " + Args.COUNT + "\n")      
      write(w, "-threshold: " + Args.normThreshold + "\n")
      write(w, "-threshold_c: " + Args.normThreshold_c + "\n")
      write(w, "-input data: " + Args.input + "\n")
      write(w, "-store final results: " + Args.STORERESULTS + "\n")
      write(w, "-pre group duplicates: " + Args.GROUPDUPLICATES + "\n")
      write(w, "###############################################################################\n\n")

      if (Args.METRICSPACE) {
        write(w, "###Metric Space:\n")
        execTimeAvg(algorithms.MetricSpace.main(args), Args.nExecs, w, writeAll)
      }      
      
      if (Args.ELEMENTSPLIT) {
        write(w, "###Element Split:\n")
        execTimeAvg(algorithms.ElementSplit.main(args), Args.nExecs, w, writeAll)
      }
      
      if (Args.INVIDXPREFETCH) {
        write(w, "###Inverted Index Prefix Filtering Fetching IDs:\n")
        execTimeAvg(algorithms.InvIdxPreFetch.main(args), Args.nExecs, w, writeAll)
      }
      
      if (Args.INVIDXPREFETCH_C) {
        write(w, "###Inverted Index Prefix Filtering Fetching IDs with near duplicates:\n")
        execTimeAvg(algorithms.InvIdxPreFetchNearDuplicates.main(args), Args.nExecs, w, writeAll)
      }
      
      if (Args.ELEMENTSPLIT_C) {
        write(w, "###Element Split with near duplicates:\n")
        execTimeAvg(algorithms.ElementSplitNearDuplicates.main(args), Args.nExecs, w, writeAll)
      }
      
      if (Args.INVIDXPRE) {
        write(w, "###Inverted Index Prefix Filtering:\n")
        execTimeAvg(algorithms.InvIdxPreFilt.main(args), Args.nExecs, w, writeAll)
      }
      
      if (Args.INVIDXFETCH) {
        write(w, "###Inverted Index Fetching IDs:\n")
        execTimeAvg(algorithms.InvIdxFetch.main(args), Args.nExecs, w, writeAll)
      }
      
      if (Args.INVIDX) {
        write(w, "###Inverted Index:\n")
        execTimeAvg(algorithms.InvIdx.main(args), Args.nExecs, w, writeAll)
      }      
            
      if (Args.BRUTEFORCE) {
        write(w, "###Brute Force:\n")
        execTimeAvg(algorithms.BruteForce.main(args), Args.nExecs, w, writeAll)
      }      
      
      now = Calendar.getInstance()
      hour = now.get(Calendar.HOUR)
      minute = now.get(Calendar.MINUTE)
      day = now.get(Calendar.DATE)
      month = now.get(Calendar.MONTH) + 1
      write(w, "\n\n###############################################################################\n")    
      write(w, "# Endet at " + hour + ":" + minute)
      write(w, " (" + day + "/" + month + ")\n")
      write(w, "###############################################################################\n")          
      
      closeWriter(w)
    }
  }
}