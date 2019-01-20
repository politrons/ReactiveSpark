

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

import scala.collection.JavaConverters._
import scala.util._


/**
  * One of the greatest things about Spark, is that we can "compose" computation results from one output
  * to another input using [binaryFiles]
  */
class SparkCompose {

  /**
    * In this example we make some computation in the first program, then we persist the results.
    * And then the PortableDataStream are obtained from the second program where we can obtain the
    * inputStream of the result, or the config of the other program with information of where it was executed and how.
    */
  @Test
  def composePrograms() {
    //Init
    val sparkContext = createSparkContext("Compose_first_program")
    val textFile: RDD[String] = sparkContext.textFile("src/main/resources/file1.txt")
    //program
    val program1 = textFile
      .flatMap(line => line.split(" "))
      .map(word => word.toUpperCase)
      .filter(word => word.length > 5)
      .map(word => {
        val value = s"##$word##"
        print(value)
        value
      })
    //Interpreters
    Try(program1.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/compose")) match {
      case Success(_) => println("Result stored")
      case Failure(exception) => println(s"Error ${exception.getMessage}")
    }
    println("---------------------- First program persisted ----------------------------")
    sparkContext.stop()

    //Second program
    val sparkContext2 = createSparkContext("Compose_second_program")

    val tupleRDD: RDD[(String, PortableDataStream)] = sparkContext2.binaryFiles("/Users/pabloperezgarcia/Development/ReactiveSpark/target/compose")
    //program
    val program2 = tupleRDD
      .map(tuple => {
        for (entry <- tuple._2.getConfiguration.iterator().asScala) {
          println(s"--------------- Previous program entry: $entry} -------------------")
        }
        tuple._2
      })
      .map(dataStream => dataStream.open().readUTF())
      .flatMap(inputStream => inputStream.split(" "))
      .filter(word => word.length > 5)
      .map(word => word.toLowerCase())
      .map(word => word.replace("##", "$$"))
    //Interpreters
    program2.foreach(println)
    sparkContext2.stop()
  }

  /**
    * Using [Union] Operator we allow Spark to create multiple programs and unite all executions.
    * Since we configure the master as [ local[4] ] it means it will use up to 4 cores so every RDD
    * program it will be executed in parallel.
    * Then he will unite one program after the other and when we persist it will be persisted one [part]
    * per program.
    */
  @Test
  def unionPrograms() {
    val sparkContext = createSparkContext("Subtract_program")
    val rdd1: RDD[String] = sparkContext.textFile("src/main/resources/story1.txt")
    val rdd2: RDD[String] = sparkContext.textFile("src/main/resources/story1.txt")
    val rdd3: RDD[String] = sparkContext.textFile("src/main/resources/story2.txt")
    val rdd4: RDD[String] = sparkContext.textFile("src/main/resources/story2.txt")

    val start = System.currentTimeMillis()
    val program1 = rdd1
      .flatMap(line => line.split(" "))
      .map(word => {
        Thread.sleep(2000)
        println(Thread.currentThread().getName)
        word.toUpperCase
      })
    val program2 = rdd2
      .flatMap(line => line.split(" "))
      .map(word => {
        Thread.sleep(2000)
        println(Thread.currentThread().getName)
        word.toUpperCase
      })
    val program3 = rdd3
      .flatMap(line => line.split(" "))
      .map(word => {
        Thread.sleep(2000)
        println(Thread.currentThread().getName)
        word.toUpperCase
      })
    val program4 = rdd4
      .flatMap(line => line.split(" "))
      .map(word => {
        Thread.sleep(2000)
        println(Thread.currentThread().getName)
        word.toUpperCase
      })
    //Interpreters
    val unionPrograms = program1.union(program2).union(program3.union(program4))
    unionPrograms.foreach(println)
    println(s"Programs execution time:${System.currentTimeMillis() - start}")
    Try(unionPrograms.saveAsTextFile("/Users/pabloperezgarcia/Development/ReactiveSpark/target/union")) match {
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
    conf.set("program", application)
    new SparkContext(conf)
  }


}