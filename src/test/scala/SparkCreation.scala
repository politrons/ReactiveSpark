

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.util._

/**
  * Here we explain how in Spark we can read data from local/HDFS(Hadoop Distributed File System)
  * Spark is Distributed programing system where we can make computation distributed in cluster and then
  * collect results from other nodes to continue the processing.
  *
  * In Spark we can apply category theory thanks to the monad RDD (RDD(Resilient Distributed Dataset))
  * the [pure] function to create the monad it would be the different operators to read files from local/HDFS as we will see here.
  *
  * Spark by default is Lazy computation like other language as Haskell. Means that when we define
  * a program and is created, is not evaluated.
  *
  * Only when we use [Actions] like [foreach][collect][reduce][fold] launch [runJob] to return a value to the user program)
  */
class SparkCreation {

  /**
    * The most simple way to create an RDD in Spark without have to read from local file system or HDFS
    * is using [parallelize] operator which acceptimng a Scala collection return the RDD ready to emitt
    * elements one by one though the pipeline.
    */
  @Test
  def parallelizeProgram() {
    val sparkContext = createSparkContext("Parallelizet_program")
    val dataRDD = sparkContext.parallelize(List("Hello", "Spark", "world"))
    //program
    dataRDD
      .map(word => word.toUpperCase)
      .map(word => s"@@___ $word ____@@")
      .foreach(println)
    sparkContext.stop()
  }

  /**
    * For this example we create a local Spark context, and use [textFile]
    * which load the text into a Spark RDD, which is a distributed representation of each line of text.
    * Spark RDD: Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,partitioned collection.
    * Only one SparkContext is allowed per JVM otherwise it throw an exception, reason by we need to stop after the program finish.
    */
  @Test
  def localTextFile() {
    val sparkContext = createSparkContext("Upper_case_program")
    val textFile: RDD[String] = sparkContext.textFile("src/main/resources/file1.txt")
    //program
    val program = textFile
      .flatMap(line => line.split(" "))
      .map(word => word.toUpperCase)
      .map(word => s"##$word##")
    //Interpreters
    program.foreach(println)
    println("Total words: " + program.count())
    Try(program.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/upperCase")) match {
      case Success(_) => println("Result stored")
      case Failure(exception) => println(s"Error ${exception.getMessage}")
    }
    sparkContext.stop()
  }

  /**
    * For this example we create a local Spark context, and use [binaryFiles] which allow read a binary result like upperCase program.
    * This operator return a RDD of tuple (path, PortableDataStream). Then using [open] over PortableDataStream
    * we get an inputStream which we read all of it using [readUTF] then we just transform in lower case again
    * and we use [filter] operator to just get the word higher than 5
    * Only one SparkContext is allowed per JVM otherwise it throw an exception, reason by we need to stop after the program finish.
    */
  @Test
  def binaryFiles() {
    //Init
    val sparkContext = createSparkContext("Lower_case_program")
    val tupleRDD: RDD[(String, PortableDataStream)] = sparkContext.binaryFiles("/Users/pabloperezgarcia/Development/ReactiveSpark/target/upperCase")
    //program
    val program = tupleRDD
      .map(tuple => tuple._2)
      .map(dataStream => dataStream.open().readUTF())
      .flatMap(inputStream => inputStream.split(" "))
      .filter(word => word.length > 5)
      .map(word => word.toLowerCase())
      .map(word => word.replace("##", "$$"))
    //Interpreters
    program.foreach(println)
    println("Total words: " + program.count())
    Try(program.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/lowerCase")) match {
      case Success(_) => println("Result stored")
      case Failure(exception) => println(s"Error ${exception.getMessage}")
    }
    sparkContext.stop()
  }

  /**
    * For this example we create a local Spark context, and use [wholeTextFiles] which read all files in one path.
    * It return a tuple of (filePath, content of file)
    * This Spark pipeline get all files get the content one by one, and we filter by only digits and then we multiply
    * by 1000
    * Only one SparkContext is allowed per JVM otherwise it throw an exception, reason by we need to stop after the program finish.
    */
  @Test
  def multiFiles() {
    //Init
    val sparkContext = createSparkContext("Just_number_program")
    val tupleRDD: RDD[(String, String)] = sparkContext.wholeTextFiles("src/main/resources/")
    //program
    val program = tupleRDD
      .map(tuple => {
        println(s"File:${tuple._1}")
        tuple._2
      })
      .flatMap(data => data.split(" "))
      .flatMap(word => word.toCharArray
        .filter(c => c.isDigit)
        .map(number => number * 1000))

    //Interpreters
    program.foreach(println)
    println("Total words: " + program.count())
    Try(program.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/numbers")) match {
      case Success(_) => println("Result stored")
      case Failure(exception) => println(s"Error ${exception.getMessage}")
    }
    sparkContext.stop()
  }

  /**
    * As we describe at the beginning, the only way to make RDD pass from lazy to eager is
    * to invoke [runJob] something that normally operators invoke implementing their own functions
    * to format the output.
    * Here we implement our own function to run the program and format the output
    */
  @Test
  def runProgram() {
    val sparkContext = createSparkContext("Upper_case_program")
    val textFile: RDD[String] = sparkContext.textFile("src/main/resources/file1.txt")
    //program
    val program = textFile
      .flatMap(line => line.split(" "))
      .map(word => word.replace("a", "A"))
      .map(word => s"##$word##")
    //Interpreters
    val array = sparkContext.runJob(program, func)
    array.foreach {
      element => println(s"Output:$element")
    }
    sparkContext.stop()
  }

  private def func[T, U]: Iterator[T] => List[U] = {
    iter => iter.map(value => value.asInstanceOf[U]).toList
  }

  /**
    * [Pipe] operator allow you to pass a script to be executed in every emission of the pipeline.
    * Here for instance we have a shell script [pipeScript] which read from terminal and print
    * the result, so since I emmit a word in every emission, the script get the word and is send it
    * back adding a exclamation mark
    */
  @Test
  def pipeScript() {
    val sparkContext = createSparkContext("Pipe_script_program")
    val dataRDD = sparkContext.parallelize(List("Hello", "Spark", "world"))
    val scriptPath = "/Users/pabloperezgarcia/Development/ReactiveSpark/src/main/resources/pipeScript.sh"
    //program
    val pipeRDD = dataRDD
      .pipe(scriptPath)
      .collect()
    pipeRDD.foreach(println)
    sparkContext.stop()
  }

  /**
    * Create a SparkContext to initialize Spark.
    * In this example we just try spark on local so we set [master] as [local]
    * We set the name of the application to  be set in the user interface.
    *
    * @return Spark context
    */
  private def createSparkContext(application: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName(application)
    new SparkContext(conf)
  }


}