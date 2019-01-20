

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.util._

/**
  * In Spark we can apply category theory thanks to the monad RDD (RDD(Resilient Distributed Dataset))
  * a monad that implement map, flatMap to make transformation and composition as we can see here.
  */
class SparkTransform {

  /**
    * Here we use [zip] operator to get three RDD, create the programs pipelines.
    * And then we zip the first two programs as a unique output program, and then finally we zip again against
    * the third program having as result tuple(content, tuple(content, content)
    */
  @Test
  def zipPrograms() {
    val sparkContext = createSparkContext("zip_program")
    val textFile1: RDD[String] = sparkContext.textFile("src/main/resources/file1.txt")
    val textFile2: RDD[String] = sparkContext.textFile("src/main/resources/file2.txt")
    val textFile3: RDD[String] = sparkContext.textFile("src/main/resources/file3.txt")
    //program 1
    val program = textFile1
      .flatMap(line => line.split(" "))
      .map(word => {
        word.toUpperCase
      })
      .map(word => s"##$word##")
    //program 2
    val program2 = textFile2
      .flatMap(line => line.split(" "))
      .map(word => {
        word.toUpperCase
      })
      .map(word => s"**$word**")
    //program 3
    val program3 = textFile3
      .flatMap(line => line.split(" "))
      .map(word => {
        word.toUpperCase
      })
      .map(word => s"@@$word@@")

    val zipProgram = program3.zip(program2.zip(program))
      .map(tuple => tuple._1 + "--------" + tuple._2._1 + "--------" + tuple._2._2)

    //Result
    zipProgram.foreach(println)
    Try(zipProgram.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/zipProgram")) match {
      case Success(_) => println("Result stored")
      case _ => println("Error:Result was already stored")
    }
    sparkContext.stop()
  }

  /**
    * Using [subtract] operator we can do two things. We can run the program, and also trasform two programs outputs
    * in one where only second arguments of second program are filter as good elements.
    */
  @Test
  def subtractPrograms() {
    val sparkContext = createSparkContext("Subtract_program")
    val rdd1: RDD[String] = sparkContext.textFile("src/main/resources/story1.txt")
    val rdd2: RDD[String] = sparkContext.textFile("src/main/resources/story2.txt")
    //program
    val program1 = rdd1
      .flatMap(line => line.split(" "))
      .map(word => word.toUpperCase)
    val program2 = rdd2
      .flatMap(line => line.split(" "))
      .map(word => word.toUpperCase)
    //Interpreters
    val distinctProgram = program1.subtract(program2)

    Try(distinctProgram.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/subtract")) match {
      case Success(_) => println("Result stored")
      case _ => println("Error:Result was already stored")
    }
    sparkContext.stop()
  }


  /**
    * Create a SparkContext to initialize Spark.
    * In this example we just try spark on local so we set [master] as [local]
    * We set the name of the application to  be set in the user interface.
    *
    * @return Spark context
    */
  def createSparkContext(application: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName(application)
    new SparkContext(conf)
  }


}