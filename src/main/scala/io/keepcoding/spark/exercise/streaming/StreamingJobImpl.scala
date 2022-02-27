package io.keepcoding.spark.exercise.streaming
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Timestamp

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingJobImpl {

  val kafkaIpServer = "34.125.250.71:9092"
  val sqlIpServer = "34.133.132.154"

   val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val struct = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id",StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false)
    ))

    dataFrame
      .select(from_json($"value".cast(StringType), struct).as("value"))
      .select($"value.*")
  }

   def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

   def enrichUserLogsWithUserMetadata(userDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    userDF.as("a")
      .join(metadataDF.as("b"), $"a.id" === $"b.id")
      .drop($"b.id")

  }

  def joinUserAntennaDF(userTotalBytesTimer: DataFrame, antennaTotalBytesTimer: DataFrame): DataFrame = {
    userTotalBytesTimer.as("a")
      .join(antennaTotalBytesTimer.as("b"), $"a.id" === $"b.id")
      .drop($"b.id")

  }

  def computeUserTotalBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "15 seconds"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("user_total_bytes"))
      .select($"window.start".as("timestamp"), $"id", $"value", $"type")
  }

  def computeAntennaTotalBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"antenna_id", window($"timestamp", "15 seconds"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("antenna_total_bytes"))
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value", $"type")
  }

  def computeAppTotalBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"app", window($"timestamp", "15 seconds"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("app_total_bytes"))
      .select($"window.start".as("timestamp"), $"app".as("id"), $"value", $"type")
  }

  def computeEmailTotalBytes(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email", $"bytes")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"email", window($"timestamp", "15 seconds"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("email_total_bytes"))
      .select($"window.start".as("timestamp"), $"email".as("id"), $"value", $"type")
  }


  def computeUserTotalBytesWithTimer(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"id", window($"timestamp", "60 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("user_total_bytes"))
      .select($"window.start".as("timestamp"),$"id", $"value", $"type")
  }

  def computeAntennaTotalBytesWithTimer(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"antenna_id", $"bytes")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"antenna_id", window($"timestamp", "60 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("antenna_total_bytes"))
      .select($"window.start".as("timestamp"),$"antenna_id".as("id"), $"value", $"type")
  }

  def computeAppTotalBytesWithTimer(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"app", window($"timestamp", "60 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("app_total_bytes"))
      .select($"window.start".as("timestamp"),$"app".as("id"), $"value", $"type")
  }

  def computeEmailTotalBytesWithTimer(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email", $"bytes")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"email", window($"timestamp", "60 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .withColumn("type", lit("email_total_bytes"))
      .select($"window.start".as("timestamp"),$"email".as("id"), $"value", $"type")
  }

  def computeQuotaLimit(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"email", $"bytes", $"quota")
      .withWatermark("timestamp","15 seconds")
      .groupBy($"email", window($"timestamp", "15 seconds"))
      .agg(
        sum($"bytes").as("usage")
      )

      .select($"email".as("email"), $"usage", $"window.start".as("timestamp"))
  }

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()
      .awaitTermination()
  }

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .withColumn("year", year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour", hour($"timestamp"))
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", storageRootPath)
      .option("checkpointLocation", "/tmp/spark-checkpoint")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {

    ////// --------------- preparacion de los datos

    val userMetadataDF =
      readUserMetadata(
        s"jdbc:postgresql://$sqlIpServer:5432/postgres",
        "user_metadata",
        "postgres",
        "keepcoding")

    val completeUserMetadata = enrichUserLogsWithUserMetadata(
        parserJsonData(
          readFromKafka(s"$kafkaIpServer", "devices")),
        userMetadataDF
      )

    computeQuotaLimit(completeUserMetadata)
      .writeStream
      .format("console")
      .start()
      .awaitTermination()

/*
    ////// --------------- append a tabla bytes

    val userTotalBytes = computeUserTotalBytes(completeUserMetadata)
    val antennaTotalBytes = computeAntennaTotalBytes(completeUserMetadata)
    val appTotalBytes = computeAppTotalBytes(completeUserMetadata)
    val emailTotalBytes = computeEmailTotalBytes(completeUserMetadata)

    writeToJdbc(userTotalBytes,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes",
      "postgres",
      "keepcoding")

    writeToJdbc(antennaTotalBytes,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes",
      "postgres",
      "keepcoding")

    writeToJdbc(appTotalBytes,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes",
      "postgres",
      "keepcoding")

    writeToJdbc(emailTotalBytes,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes",
      "postgres",
      "keepcoding")


    ////// --------------- append a tabla bytes_hourly

    val userTotalBytesTimer = computeUserTotalBytesWithTimer(completeUserMetadata)
    val antennaTotalBytesTimer = computeAntennaTotalBytesWithTimer(completeUserMetadata)
    val appTotalBytesTimer = computeAppTotalBytesWithTimer(completeUserMetadata)
    val emailTotalBytesTimer = computeEmailTotalBytesWithTimer(completeUserMetadata)

    writeToJdbc(userTotalBytesTimer,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding")

    writeToJdbc(antennaTotalBytesTimer,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding")

    writeToJdbc(appTotalBytesTimer,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding")

    writeToJdbc(emailTotalBytesTimer,
      s"jdbc:postgresql://sqlIpServer:5432/postgres",
      "bytes_hourly",
      "postgres",
      "keepcoding")
*/
/*

 */
  }
}
