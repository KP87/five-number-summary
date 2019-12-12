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

  //  Comparison of few methods to calculate quartiles values
  runWithTimeOfExecution(quartileBySQL("lng"))
  runWithTimeOfExecution(approxQuartile("lng"))
  runWithTimeOfExecution(quartileBySQLApprox("lng"))
  runWithTimeOfExecution(quartileByWindow("lng"))
  runWithTimeOfExecution(quartileByDataFrame("lng"))
  runWithTimeOfExecution(quartileByRdd("lng"))
  runWithTimeOfExecution(quartileByRddFilter("lng"))
  runWithTimeOfExecution(quartileByRddFilterInLoop("lng"))

  //  Comparison of few methods to calculate 'five summary values' values
  runWithTimeOfExecution(fiveNumberSummaryBySQL(allDataDF,"lng", "lat"))
  runWithTimeOfExecution(fiveNumberSummary(allDataDF,"lng", "lat"))

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

  // Methods to calculate quartiles values
  def quartileByDataFrame(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val count = allDataDF.count.toInt
    val firstQuartileIndex = Math.round(0.25 * count.toDouble).toInt
    val secondQuartileIndex = Math.round(0.5 * count.toDouble).toInt
    val thirdQuartileIndex = Math.round(0.75 * count.toDouble).toInt

    val lngDF: Dataset[Row] = allDataDF.select(columnName).sort(columnName)

    val thirdQuartileArray: Array[Row] = lngDF.take(thirdQuartileIndex)
    val thirdQuartileValue: Double = thirdQuartileArray.last.getDouble(0)

    val secondQuartileArray: Array[Row] = thirdQuartileArray.take(secondQuartileIndex)
    val secondQuartileValue: Double = secondQuartileArray.last.getDouble(0)

    val firstQuartileArray: Array[Row] = secondQuartileArray.take(firstQuartileIndex)
    val firstQuartileValue: Double = firstQuartileArray.last.getDouble(0)

    Array(firstQuartileValue, secondQuartileValue, thirdQuartileValue)
  }

  def quartileByRdd(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val lngDF: RDD[(Long, Double)] = allDataDF
      .select(columnName)
      .rdd
      .map(x => x.getDouble(0))
      .sortBy(x => x)
      .zipWithIndex()
      .map(_.swap)

    val count = lngDF.count

    val firstQuartileIndex = Math.round(0.25 * count.toDouble)
    val secondQuartileIndex = Math.round(0.5 * count.toDouble)
    val thirdQuartileIndex = Math.round(0.75 * count.toDouble)

    val firstQuartileValue: Double = lngDF.lookup(firstQuartileIndex).head
    val secondQuartileValue: Double = lngDF.lookup(secondQuartileIndex).head
    val thirdQuartileValue: Double = lngDF.lookup(thirdQuartileIndex).head

    Array(firstQuartileValue, secondQuartileValue, thirdQuartileValue)
  }

  def quartileByRddFilter(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val lngDF: RDD[(Double, Long)] = allDataDF
      .select(columnName)
      .rdd
      .map(x => x.getDouble(0))
      .sortBy(x => x)
      .zipWithIndex()

    val count = lngDF.count

    val firstQuartileIndex = Math.round(0.25 * count.toDouble)
    val secondQuartileIndex = Math.round(0.5 * count.toDouble)
    val thirdQuartileIndex = Math.round(0.75 * count.toDouble)

    val firstQuartileValue = lngDF.filter(_._2 == firstQuartileIndex).first()._1
    val secondQuartileValue: Double = lngDF.filter(_._2 == secondQuartileIndex).first()._1
    val thirdQuartileValue: Double = lngDF.filter(_._2 == thirdQuartileIndex).first()._1

    Array(firstQuartileValue, secondQuartileValue, thirdQuartileValue)
  }

  def quartileByRddFilterInLoop(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    import spark.implicits._
    val lngDF: RDD[(Double, Long)] = allDataDF
      .map(x => x.getDouble(2))
      .rdd
      .sortBy(x => x)
      .zipWithIndex()

    val count: Long = lngDF.count

    val output: Seq[Double] = for(percentile <- 25 to 75 by 25) yield {
      lngDF.filter(_._2 == Math.round(percentile/100.0 * count.toDouble).toInt).first()._1
    }
    output.toArray
  }

  def quartileByWindow(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val lngDF: DataFrame = allDataDF
      .select("lng")
      .withColumn("percentile", percent_rank() over Window.orderBy(columnName))

    val firstQuartileValue: Double = lngDF.filter("percentile >= 0.25").head().getDouble(0)
    val secondQuartileValue: Double = lngDF.filter("percentile >= 0.5").head().getDouble(0)
    val thirdQuartileValue: Double = lngDF.filter("percentile >= 0.75").head().getDouble(0)

    Array(firstQuartileValue, secondQuartileValue, thirdQuartileValue)
  }

  def approxQuartile(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    allDataDF.stat.approxQuantile(columnName, Array(0.25,0.5,0.75),0.0001)
  }

  def quartileBySQL(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val viewName = tableName + "_tmp"
    allDataDF.createOrReplaceTempView(viewName)

    spark
      .sql(s"SELECT percentile($columnName, array(0.25,0.5,0.75)) as quartile FROM $viewName")
      .head()
      .getSeq[Double](0)
      .toArray
  }

  def quartileBySQLApprox(columnName:String): Array[Double] = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val viewName = tableName + "_tmp"
    allDataDF.createOrReplaceTempView(viewName)

    spark
      .sql(s"SELECT percentile_approx($columnName, array(0.25,0.5,0.75), 100) as quartile FROM $viewName")
      .head()
      .getSeq[Double](0)
      .toArray
  }

  // Methods to calculate 'five summary values' values
  def fiveNumberSummary(dataFrame: DataFrame, columns:String*): DataFrame = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    dataFrame.select(columns.head, columns.tail: _*).summary("min", "25%", "50%", "75%", "max")
  }

  def fiveNumberSummaryBySQL(dataFrame: DataFrame, columns:String*): DataFrame = {
    println(s"Function name:\t${Thread.currentThread.getStackTrace()(1).getMethodName}")

    val viewName = tableName + "_tmp"
    allDataDF.createOrReplaceTempView(viewName)

    val input: Seq[(Int, Seq[String])] = List(
      (0, "min" :: Nil),
      (1, "first quartile" :: Nil),
      (2, "median" :: Nil),
      (3, "third quartile" :: Nil),
      (4, "max" :: Nil))

    def query(input: Seq[(Int, Seq[String])], columns:List[String]):Seq[(Int, Seq[String])] = columns match {
      case Nil => input
      case head :: tail => {
        val agg = spark
          .sql(s"SELECT MIN($head) as min, percentile_approx($head, array(0.25,0.5,0.75), 100) as quartile, MAX($head) as max FROM $viewName")
          .head()

        val queryOutput: Seq[Double] = agg.getDouble(0) +: agg.getSeq[Double](1) :+ agg.getDouble(2)

        val output = input.map(item => (item._1, item._2 :+ queryOutput(item._1).toString))

        query(output, tail)
      }
    }

    val summaryRDD: RDD[Row] = spark.sparkContext.parallelize(query(input, columns.toList).map(item =>  Row.fromSeq(item._2)))

    val fields: Array[StructField] =
      StructField("summary", StringType, true) +:
        columns.map(columnName => StructField(columnName, StringType, true)).toArray

    val schema: StructType = new StructType(fields)

    spark.createDataFrame(summaryRDD, schema)
  }
}
