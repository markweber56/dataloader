package main.scala.dataloader

import org.apache.spark.sql.{SparkSession, SaveMode}
import java.io.File
import org.apache.spark.sql.functions._

object DataLoader extends Configurable {

  def main(args: Array[String]): Unit = {

    val config = getConfig("config.json")
    val dbConfig = config.database
    val dataDir: String = "data//price"
    val tableName = "raw.price"

    val serverName = dbConfig.server;
    val serverPort = dbConfig.port;
    val dbName = dbConfig.dbName;

    val jdbcUrl = "jdbc:postgresql://" + serverName + ":" + serverPort + "/" + dbName

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("dataloader")
      .getOrCreate()

    println("starting dataloader")

    val fileNames: Array[String] = readFilesFromDirectory(dataDir)

    val sampleFile = fileNames(2)
    println(s"processing file: $sampleFile")

    val schema = TableSchemas.priceSchema

    val priceDF = spark.read.schema(schema).csv(sampleFile)

    val priceDF2 = priceDF.drop("closing_bid2")
      .filter(col("open").isNotNull)
      .filter(col("trade_quantity").isNotNull)

    priceDF2.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", dbConfig.dbUser)
      .option("password", dbConfig.dbPassword)
      .option("driver", dbConfig.driver)
      .save()

    spark.stop()
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