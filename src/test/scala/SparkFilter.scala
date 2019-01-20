

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.util.{Success, Try}

/**
  * To filter in Spark it's pretty much like in any other Functional API, filter, distinct, take, groupBy
  * and some other, but are not so extended like in other API like ReactiveX or Reactor.
  */
class SparkFilter {

  /**
    * Some of the most famous filters, operators to get rid of elements in the emission of your pipeline.
    */
  @Test
  def filtersProgram() {
    val sparkContext = createSparkContext("Filters_program")
    val rddText: RDD[String] = sparkContext.textFile("src/main/resources/file1.txt")
    //program
    val program = rddText
      .flatMap(data => data.split(" "))
      .map(word => word.toUpperCase)
      .filter(word => word.length > 5)
      .distinct
      .take(100)
    //Interpreters
    program.foreach(println)
    sparkContext.stop()
  }

  /**
    * Using [distinct] operator we can  filter only for elements not repeated in the stream.
    */
  @Test
  def distinctProgram() {
    val sparkContext = createSparkContext("Distinct_program")
    val rdd1: RDD[String] = sparkContext.textFile("src/main/resources/distinct.txt")
    //program
    val distinctProgram = rdd1
      .flatMap(line => line.split(" "))
      .map(word => word.toUpperCase)
      .distinct()
    distinctProgram.foreach(println)
    //Interpreters
    Try(distinctProgram.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/distinct")) match {
      case Success(_) => println("Result stored")
      case _ => println("Error:Result was already stored")
    }
    sparkContext.stop()
  }


  /**
    * Using [groupBy] operator it return a new RDD of tuple(Name_of_group, Iterable[T])
    * this name of group it passed as return argument of the function
    */
  @Test
  def groupByProgram() {
    val sparkContext = createSparkContext("GroupBy_program")
    val rdd1: RDD[String] = sparkContext.textFile("src/main/resources/story1.txt")
    //program
    val groupProgram1 = rdd1
      .flatMap(line => line.split(" "))
      .map(word => word.toUpperCase)
      .groupBy(_ => "CUSTOM_GROUP1")

    val groupProgram2 = rdd1
      .flatMap(line => line.split(" "))
      .map(word => word.toUpperCase)
      .groupBy(_ => "CUSTOM_GROUP2")

    //Interpreters
    groupProgram1.foreach(println)
    groupProgram2.foreach(println)
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