package main.scala.dataloader

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._

object DataLoader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("dataloader")
      .getOrCreate()

    println("starting dataloaders")
  }

}
