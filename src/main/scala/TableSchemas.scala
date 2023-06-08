package main.scala.dataloader

import org.apache.spark.sql.types._

object TableSchemas {

  val priceSchema = StructType(Array(StructField("instrument_key", IntegerType, false),
    StructField("trade_date", DateType, false),
    StructField("open", FloatType, false),
    StructField("closing_ask", FloatType, false),
    StructField("closing_bid", FloatType, false),
    StructField("closing_bid2", FloatType, false),
    StructField("close", FloatType, false),
    StructField("highest", FloatType, false),
    StructField("lowest", FloatType, false),
    StructField("trade_quantity", IntegerType, false)))

}
