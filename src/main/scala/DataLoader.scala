package main.scala.dataloader

import org.apache.spark.sql.{SparkSession, SaveMode}
import java.io.File
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DataLoader {

  def main(args: Array[String]): Unit = {

    val dataDir: String = "data//price"

    val jdbcUrl = "jdbc:postgresql://localhost:5432/marketdb"
    val tableName = "raw.price"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("dataloader")
      .getOrCreate()

    println("starting dataloader")

    val fileNames: Array[String] = readFilesFromDirectory(dataDir)
    println(fileNames(0))

    val sampleFile = fileNames(0)

    val schema = StructType(Array(StructField("instrument_key", IntegerType, false),
      StructField("trade_date", DateType, false),
      StructField("open", FloatType, false),
      StructField("closing_ask", FloatType, false),
      StructField("closing_bid", FloatType, false),
      StructField("closing_bid2", FloatType, false),
      StructField("close", FloatType, false),
      StructField("highest", FloatType, false),
      StructField("lowest", FloatType, false),
      StructField("trade_quantity", IntegerType, false)))

    val priceDF = spark.read.schema(schema).csv(sampleFile)

    val priceDF2 = priceDF.drop("closing_bid2")
      .filter(col("open").isNotNull)
      .filter(col("trade_quantity").isNotNull)

    priceDF2.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", "super")
      .option("password", "1010TeraB!")
      .option("driver", "org.postgresql.Driver")
      .save()

    spark.stop()

    // priceDF2.show(10)


  }

  def readFilesFromDirectory(directoryPath: String): Array[String] = {
    val directory = new File(directoryPath)

    if (directory.exists && directory.isDirectory) {
      directory.listFiles.filter(_.isFile).map(f => f.toString())
    } else {
      Array.empty[String]
    }
  }

}