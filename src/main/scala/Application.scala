import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Application extends App {
  val spark = SparkSession
    .builder()
    .appName("FiveNumberSummary")
    .config("spark.master", "local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("Error")

  val url = "jdbc:postgresql://127.0.0.1:5432/test_dataset"
  val tableName: String = "beijing_taxi"
  val connectionProperties = new Properties()
  connectionProperties.put("user", "postgres")
  connectionProperties.put("driver", "org.postgresql.Driver")

  val allDataDF: DataFrame = spark.read.jdbc(url, tableName, connectionProperties)

  // Comparison of few methods to calculate min and max value
  runWithTimeOfExecution(minMaxBySQL("lng"))
  runWithTimeOfExecution(minMaxBySQLFunctions("lng"))
  runWithTimeOfExecution(minMaxGroupBy("lng"))
  runWithTimeOfExecution(minMaxByRdd("lng"))
  runWithTimeOfExecution(minMaxBySummary("lng"))

  def runWithTimeOfExecution[A](anyFunction: => A): Unit = {
    val startTime = System.nanoTime().toDouble
    val output = anyFunction
    println(s"Executed in:\t${(System.nanoTime().toDouble - startTime)/1000000000} s")

    println("Result:")
    output match {
      case x:Array[Double] => x.foreach(println)
      case x:DataFrame => println(x.show)
      case _ => println("Unknown type!")
    }
    println("\n")
  }

  // Methods to calculate min and max value
  def minMaxBySummary(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val describedDF = allDataDF.summary("min", "max")
    val lngMin = describedDF.filter("summary LIKE 'min'").select(columnName).head().getString(0).toDouble
    val lngMax = describedDF.filter("summary LIKE 'max'").select(columnName).head().getString(0).toDouble
    Array(lngMin, lngMax)
  }

  def minMaxBySQL(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val viewName = tableName + "_tmp"
    allDataDF.createOrReplaceTempView(viewName)
    val agg: Row = spark.sql(s"SELECT MIN($columnName) as min, MAX($columnName) as max FROM $viewName").head()
    Array(agg.getDouble(0), agg.getDouble(1))
  }

  def minMaxBySQLFunctions(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    import org.apache.spark.sql.functions.{max, min}
    val agg: Row = allDataDF.agg(min(columnName).alias("min"), max(columnName).alias("max")).head()
    Array(agg.getDouble(0), agg.getDouble(1))
  }

  def minMaxGroupBy(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val test = allDataDF.groupBy()
    val min = test.min(columnName).head().getDouble(0)
    val max = test.max(columnName).head().getDouble(0)
    Array(min, max)
  }

  def minMaxByRdd(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val agg = allDataDF.select(columnName).rdd
    val min = agg.reduce((a, b) => if(a.getDouble(0) < b.getDouble(0)) a else b).getDouble(0)
    val max = agg.reduce((a, b) => if(a.getDouble(0) > b.getDouble(0)) a else b).getDouble(0)
    Array(min, max)
  }
}
