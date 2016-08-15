package utils

import scala.xml.XML

object Args {
  var DEBUG = false
  var PROFILING = false
  var COUNT = false
  var CREATEDATA = false
  
  var WRITEALL = true
  var INIT = true
  var BRUTEFORCE = true
  var ELEMENTSPLIT = true
  var INVIDX = true
  var INVIDXPRE = true
  var INVIDXFETCH = true
  var INVIDXPREFETCH = true
  var BENCHMARK = true
  var benchmarkOutput = ""
  
  var nExecs = 2  
  var configFilePath = ""
  var k = 0
  var n = 0
  var distinctElements = 0
  var dataSetPath = ""
  var threshold = 0.toLong
  var normThreshold = 0.0
  var minOverlap = 0.toLong
  var nPools = 0
  var selectivity = 0.0
  var poolIntersection = 0.0
  var input = ""
  var output = ""
  var datasetOutput = ""
  var masterIp = "local"
  
  var partitions = 1
  var cores = "1"
  var executors = "1"
  
  val usage = """
usage: class [options] ...
classes:
   algorithms.Init
   benchmark.Benchmark
   benchmark.SyntheticDataSet
options: 
   --k                N    : ranking size
   --n                N    : number of rankings
   --threshold        N.M  : normalized similarity threshold
   --selectivity      N.M  : selectivity percentage
   --poolIntersection N.M  : intersection percentage
   --nPools           N    : number of pools for intersecting rankings
   --nElements        N    : number of distinct elements
   --config           PATH : path to XML configuration file
   --input            PATH : input dataset path
   --output           PATH : result output path
   --datasetOutput    PATH : dataset output path (when creating new ones)
   --benchmarkOutput  PATH : benchmarking results output path
   --count            BOOL : count number of result pairs
   --debug            BOOL : debug mode
   --profiling        BOOL : profiling mode
   --createData       BOOL : create synthetic dataset
   --partitions       N    : number of partitions for repartitioning
   --cores            N    : number of cores to use on local machine
   --executors        N    : number of executors on local machine
   --masterIp         IP   : master node IP
   --nExecs           N    : number of executions of each algorithm
   --writeAll         BOOL : write execution time for each execution
   --init             BOOL : run Spark context initialization
   --bruteforce       BOOL : run brute force
   --elementsplit     BOOL : run elementsplit
   --invidx           BOOL : run inverted index
   --invidxpre        BOOL : run inverted index prefix filtering
   --invidxfetch      BOOL : run inverted index fetching IDs
   --invidxprefetch   BOOL : run inverted index prefix filtering fetch ID
   --benchmark        BOOL : run benchmarking (false dont run any approach)
  """
  
  /**
   * Set ranking size K. Used for automatically know its size, no
   * need to give it as argument or know it in advance.
   */
  def setK (k: Int) {
    this.k = k
    // Update denormalized threshold
    this.threshold = Footrule.denormalizeThreshold(k, normThreshold)
    // Update minimum overlap between ranks
    this.minOverlap = Footrule.getMinOverlap(k, normThreshold)
  }
  
  /**
   * This object parses all possible parameters to all algorithms
   * including parsing configuration XML file. Parsed arguments are
   * accessible through global variables defined on the top of this
   * file.
   */
  def parse(args: Array[String]) {
    
    if (args.length == 0)  {
      println(usage)
    }
    
    val arglist = args.toList
    
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map

        case "--k" :: value :: tail =>
                               nextOption(map ++ Map('k -> value.toInt), tail)
        case "--n" :: value :: tail =>
                               nextOption(map ++ Map('n -> value.toInt), tail)
        case "--threshold" :: value :: tail =>
                               nextOption(map ++ Map('threshold -> value.toDouble), tail)
        case "--selectivity" :: value :: tail =>
                               nextOption(map ++ Map('selectivity -> value.toDouble), tail)
        case "--poolIntersection" :: value :: tail =>
                               nextOption(map ++ Map('poolIntersection -> value.toDouble), tail)
        case "--nPools" :: value :: tail =>
                               nextOption(map ++ Map('nPools -> value.toInt), tail)                               
        case "--nElements" :: value :: tail =>
                               nextOption(map ++ Map('nElements -> value.toInt), tail)                               
        case "--config" :: value :: tail =>
                               nextOption(map ++ Map('config -> value.toString()), tail)
        case "--input" :: value :: tail =>
                               nextOption(map ++ Map('input -> value.toString()), tail)
        case "--output" :: value :: tail =>
                               nextOption(map ++ Map('output -> value.toString()), tail)
        case "--datasetOutput" :: value :: tail =>
                               nextOption(map ++ Map('datasetOutput -> value.toString()), tail)
        case "--benchmarkOutput" :: value :: tail =>
                               nextOption(map ++ Map('benchmarkOutput -> value.toString()), tail)                                
        case "--count" :: value :: tail =>
                               nextOption(map ++ Map('count -> value.toBoolean), tail)
        case "--debug" :: value :: tail =>
                               nextOption(map ++ Map('debug -> value.toBoolean), tail)
        case "--profiling" :: value :: tail =>
                               nextOption(map ++ Map('profiling -> value.toBoolean), tail)                               
        case "--createData" :: value :: tail =>
                               nextOption(map ++ Map('createData -> value.toBoolean), tail)
        case "--nExecs" :: value :: tail =>
                               nextOption(map ++ Map('nExecs -> value.toInt), tail)                               
        case "--writeAll" :: value :: tail =>
                               nextOption(map ++ Map('writeAll -> value.toBoolean), tail)                                   
        case "--partitions" :: value :: tail =>
                               nextOption(map ++ Map('partitions -> value.toInt), tail)
        case "--cores" :: value :: tail =>
                               nextOption(map ++ Map('cores -> value.toString()), tail)
        case "--executors" :: value :: tail =>
                               nextOption(map ++ Map('executors -> value.toString()), tail)
        case "--masterIp" :: value :: tail =>
                               nextOption(map ++ Map('masterIp -> value.toString()), tail)
        case "--benchmark" :: value :: tail =>
                               nextOption(map ++ Map('benchmark -> value.toBoolean), tail)  
        case "--init" :: value :: tail =>
                               nextOption(map ++ Map('init -> value.toBoolean), tail)
        case "--bruteforce" :: value :: tail =>
                               nextOption(map ++ Map('bruteforce -> value.toBoolean), tail)                               
        case "--invidx" :: value :: tail =>
                               nextOption(map ++ Map('invidx -> value.toBoolean), tail)
        case "--invidxpre" :: value :: tail =>
                               nextOption(map ++ Map('invidxpre -> value.toBoolean), tail)  
        case "--invidxfetch" :: value :: tail =>
                               nextOption(map ++ Map('invidxfetch -> value.toBoolean), tail)  
        case "--invidxprefetch" :: value :: tail =>
                               nextOption(map ++ Map('invidxprefetch -> value.toBoolean), tail)  
        case "--elementsplit" :: value :: tail =>
                               nextOption(map ++ Map('elementsplit -> value.toBoolean), tail)                                 
        case option :: tail => println("Unknown option " + option + usage)
                               exit(1) 
      }
    }
    
    val options = nextOption(Map(),arglist)
  
    // Parameters from XML file
    if (options.get('config).isDefined) {
      var configXml = XML.loadFile(options.get('config).mkString)
      COUNT = (((configXml \\ "config") \\ "storeCount").text).toBoolean
      masterIp = ((configXml \\ "config") \\ "masterIp").text
      k = ((((configXml \\ "config") \\ "dataSet") \\ "k").text).toInt
      n = ((((configXml \\ "config") \\ "dataSet") \\ "n").text).toInt
      distinctElements = ((((configXml \\ "config") \\ "dataSet") \\ "distinctElements").text).toInt
      dataSetPath = (((configXml \\ "config") \\ "dataSet") \\ "output").text
      normThreshold = (((configXml \\ "config") \\ "threshold").text).toDouble
      nPools = ((((configXml \\ "config") \\ "dataSet") \\ "nPool").text).toInt
      selectivity = ((((configXml \\ "config") \\ "dataSet") \\ "selectivity").text).toDouble
      poolIntersection = ((((configXml \\ "config") \\ "dataSet") \\ "poolIntersection").text).toDouble
      DEBUG = (((configXml \\ "config") \\ "debug").text).toBoolean
      CREATEDATA = ((((configXml \\ "config") \\ "dataSet") \\ "createData").text).toBoolean
      input = ((configXml \\ "config") \\ "input").text
      output = ((configXml \\ "config") \\ "outputFolder").text
      partitions = (((configXml \\ "config") \\ "partitions").text).toInt
      datasetOutput = (((configXml \\ "config") \\ "dataSet") \\ "output").text
      
      if (DEBUG) {
        println(configXml)
      }      
    }   
    
    // Overwrite with passed by argument values    
    if (options.get('k).isDefined)
      k = options.get('k).mkString.toInt
      
    if (options.get('n).isDefined)
      n = options.get('n).mkString.toInt
      
    if (options.get('nExecs).isDefined)
      nExecs = options.get('nExecs).mkString.toInt      
      
    if (options.get('threshold).isDefined) {
      normThreshold = options.get('threshold).mkString.toDouble
      threshold = Footrule.denormalizeThreshold(k, normThreshold)
      minOverlap = Footrule.getMinOverlap(Args.k, normThreshold)
    }
      
    if (options.get('selectivity).isDefined)
      selectivity = options.get('selectivity).mkString.toDouble
      
    if (options.get('poolIntersection).isDefined)
      poolIntersection = options.get('poolIntersection).mkString.toDouble      

    if (options.get('nElements).isDefined)
      distinctElements = options.get('nElements).mkString.toInt
      
    if (options.get('nPools).isDefined)
      nPools = options.get('nPools).mkString.toInt      
    
    if (options.get('count).isDefined)
      COUNT = options.get('count).mkString.toBoolean
      
    if (options.get('writeAll).isDefined)
      WRITEALL = options.get('writeAll).mkString.toBoolean      

    if (options.get('debug).isDefined)
      DEBUG = options.get('debug).mkString.toBoolean

    if (options.get('profiling).isDefined)
      PROFILING = options.get('profiling).mkString.toBoolean
      
    if (options.get('createData).isDefined)
      CREATEDATA = options.get('createData).mkString.toBoolean
      
    if (options.get('benchmark).isDefined)
      BENCHMARK = options.get('benchmark).mkString.toBoolean      

    if (options.get('input).isDefined)
      input = options.get('input).mkString      

    if (options.get('output).isDefined)
      output = options.get('output).mkString

    if (options.get('benchmarkOutput).isDefined)
      benchmarkOutput = options.get('benchmarkOutput).mkString
      
    if (options.get('masterIp).isDefined)
      masterIp = options.get('masterIp).mkString      
      
    if (options.get('partitions).isDefined)
      partitions = options.get('partitions).mkString.toInt
      
    if (options.get('cores).isDefined)
      cores = options.get('cores).mkString
      
    if (options.get('executors).isDefined)
      executors = options.get('executors).mkString      

    if (options.get('datasetOutput).isDefined)
      datasetOutput = options.get('datasetOutput).mkString   
      
    if (options.get('benchmark).isDefined)
      BENCHMARK = options.get('benchmark).mkString.toBoolean 
      
    if (options.get('init).isDefined)
      INIT = options.get('init).mkString.toBoolean
      
    if (options.get('bruteforce).isDefined)
      BRUTEFORCE = options.get('bruteforce).mkString.toBoolean 
      
    if (options.get('invidx).isDefined)
      INVIDX = options.get('invidx).mkString.toBoolean
      
    if (options.get('invidxpre).isDefined)
      INVIDXPRE = options.get('invidxpre).mkString.toBoolean 
      
    if (options.get('invidxfetch).isDefined)
      INVIDXFETCH = options.get('invidxfetch).mkString.toBoolean

    if (options.get('invidxprefetch).isDefined)
      INVIDXPREFETCH = options.get('invidxprefetch).mkString.toBoolean

    if (options.get('elementsplit).isDefined)
      ELEMENTSPLIT = options.get('elementsplit).mkString.toBoolean      
      
    if (DEBUG) {
      println(options)
    }
  }  
}