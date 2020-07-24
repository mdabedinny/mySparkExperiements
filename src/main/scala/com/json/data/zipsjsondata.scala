package com.json.data

import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructType}


object zipsjsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("zipsjsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\datasets\\zips.json"
    //val jsonSchema
    val jsonSchema = new StructType()
      .add("_id", IntegerType)
      .add("city", StringType)
      .add("pop", LongType)
      .add("state", StringType)
      .add("loc", new StructType().add("lati", DoubleType).add("lang", DoubleType))

    val df = spark.read.format("json").schema(jsonSchema).load(data)
    df.show(5)
    df.printSchema()

    spark.stop()
  }
}
