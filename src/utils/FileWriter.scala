package utils

import java.io._

import org.apache.hadoop.conf.Configuration;
import java.net._
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

object FileWriter {
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
   * Write executino time to folder
   */
  def writeExecTime(writer: Either[BufferedWriter, PrintWriter], execTime: Long) {
    write(writer, "[BENCHMARK] " + "%20d".format(execTime) + " ns: Execution time\n")    
  }
  
  def writeFinalStatus(writer: Either[BufferedWriter, PrintWriter], hdfsUri: String, outputPath: String) {
    if (outputPath.contains("hdfs")) {    
      val conf = new Configuration()
      val hdfs = org.apache.hadoop.fs.FileSystem.get( new URI( hdfsUri ), conf );
      val file = new Path(outputPath + "/_SUCCESS");
      
      if (hdfs.exists(file))
        write(writer, "[BENCHMARK]  TASK SUCCESS")
      else
        write(writer, "[BENCHMARK]  TASK FAILED")
    }
    else {
      if (new File(outputPath + "/_SUCCESS").exists())
        write(writer, "[BENCHMARK]  TASK SUCCESS")
      else
        write(writer, "[BENCHMARK]  TASK FAILED")
    }
  }
}