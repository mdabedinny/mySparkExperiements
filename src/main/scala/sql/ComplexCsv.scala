package sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ComplexCsv {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ComplexCsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql

    val data = "C:\\work\\datasets\\datasets\\10000Records.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("path",data).load()
    //  df.show()
    // if u know sql use sql or if u know scala use scala operations ...
    df.createOrReplaceTempView("tab") // it allows sql queries on top of dataframe
    val res = spark.sql("select * from tab where limit 3 ")
    res.show()
    //cleaning processing
    val reg = "[^a-zA-Z0-9]"
    val cols = df.columns.map(x=>x.replaceAll(reg, "").toLowerCase())
    val ndf = df.toDF(cols:_*)

    ndf.show()

    spark.stop()
  }
}
