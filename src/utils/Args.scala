package utils

import scala.xml.XML

object Args {
  
  var DEBUG = false
  var COUNT = false
  var CREATEDATA = false
  
  var configXml = XML.loadFile("config/config0.xml")
  var k = 0
  var n = 0
  var distinctElements = 0
  var dataSetPath = ""
  var normThreshold = 0.0
  var nPools = 0
  var selectivity = 0.0
  var poolIntersection = 0.0
  var input = ""
  var output = ""
  var datasetOutput = ""
  
  var nodes = 1
  
  val usage = """
    Usage: mmlaln [--min-size num] [--max-size num] filename
  """
  
  /**
   * This object parses all possible parameters to all algorithms
   * including parsing configuration XML file. Parsed arguments are
   * accessible through global variables defined on the top of this
   * file.
   */
  def parse(args: Array[String]) {  
    
    if (args.length == 0) println(usage)
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
        case "--count" :: value :: tail =>
                               nextOption(map ++ Map('count -> value.toBoolean), tail)
        case "--debug" :: value :: tail =>
                               nextOption(map ++ Map('debug -> value.toBoolean), tail)
        case "--createData" :: value :: tail =>
                               nextOption(map ++ Map('createData -> value.toBoolean), tail) 
        case "--nodes" :: value :: tail =>
                               nextOption(map ++ Map('nodes -> value.toInt), tail)                               
        /*case string :: opt2 :: tail if isSwitch(opt2) => 
                               nextOption(map ++ Map('infile -> string), list.tail)
        case string :: opt2 :: tail if isSwitch(opt2) => 
                               nextOption(map ++ Map('infile -> string), list.tail)                               
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)*/
        case option :: tail => println("Unknown option "+option)
                               exit(1) 
      }
    }
    
    val options = nextOption(Map(),arglist)
    
    if (options.get('createData).isDefined)
      CREATEDATA = options.get('createData).mkString.toBoolean      
   
    // Parameters from XML file
    if (options.get('config).isDefined) {
      configXml = XML.loadFile(options.get('config).mkString)
      COUNT = (((configXml \\ "config") \\ "storeCount").text).toBoolean
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
      nodes = (((configXml \\ "config") \\ "nodes").text).toInt
      datasetOutput = (((configXml \\ "config") \\ "dataSet") \\ "output").text
    }
    
    // Overwrite with passed by argument values    
    if (options.get('k).isDefined)
      k = options.get('k).mkString.toInt
      
    if (options.get('n).isDefined)
      n = options.get('n).mkString.toInt
      
    if (options.get('threshold).isDefined)
      normThreshold = options.get('threshold).mkString.toDouble   

    if (options.get('nElements).isDefined)
      distinctElements = options.get('nElements).mkString.toInt       
    
    if (options.get('count).isDefined)
      COUNT = options.get('count).mkString.toBoolean

    if (options.get('debug).isDefined)
      DEBUG = options.get('debug).mkString.toBoolean

    if (options.get('input).isDefined)
      input = options.get('input).mkString      

    if (options.get('output).isDefined)
      output = options.get('output).mkString 
      
    if (options.get('nodes).isDefined)
      nodes = options.get('nodes).mkString.toInt

    if (options.get('datasetOutput).isDefined)
      datasetOutput = options.get('datasetOutput).mkString      
      
    if (DEBUG) {
      println(options)      
      if (options.get('config).isDefined) {
        println(configXml)
      }
    }
  }  
}