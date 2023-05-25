package main.scala.dataloader

import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.types._

object DataLoader {

  def main(args: Array[String]): Unit = {

    val dataDir: String = "data//price"

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("dataloader")
      .getOrCreate()

    println("starting dataloader")

    val fileNames: Array[String] = readFilesFromDirectory(dataDir)
    println(fileNames(0))

    val sampleFile = fileNames(0)

    val schema = StructType(Array(StructField("InstrumentKey", IntegerType, false),
      StructField("TradeDt", StringType, false),
      StructField("OpenPriceAmt", FloatType, false),
      StructField("ClosingAskPriceAmt", FloatType, false),
      StructField("ClosingBidPriceAmt", FloatType, false),
      StructField("ClosingBidPriceAmt2", FloatType, false),
      StructField("ClosingPriceAmt", FloatType, false),
      StructField("HighestTradingPriceAmt", FloatType, false),
      StructField("LowestPriceAmt", FloatType, false),
      StructField("NumberOfTradesQty", IntegerType, false)))

    val priceDF = spark.read.schema(schema).csv(sampleFile)

    priceDF.show(10)


  }

  def readFilesFromDirectory(directoryPath: String): Array[String] = {
    val directory = new File(directoryPath)

    if (directory.exists && directory.isDirectory) {
      println("directory exists")
      val files = directory.listFiles.filter(_.isFile)
      files.map(f => f.toString())
    } else {
      print("directory does not exist")
      Array.empty[String]
    }
  }

}