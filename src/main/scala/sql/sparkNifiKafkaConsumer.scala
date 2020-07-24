package sql

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object sparkNifiKafkaConsumer {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("KafkaConsumer").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext
    val output ="C:\\work\\kafkaop"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("indpak")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(record =>  record.value)
    //lines.saveAsTextFiles(output)
   lines.print()
    lines.foreachRDD{ rdd=>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = spark.read.json(rdd)
    //  df.show()
     // df.printSchema()

      df.createOrReplaceTempView("tab")
      val res = spark.sql("select nationality, r.user.cell mobile, r.user.email email, r.user.location.city citi from tab lateral view explode(results) t as r")
      res.show()


       val url = "jdbc:oracle:thin:@//corona2020.cngsgxxckbvf.us-east-2.rds.amazonaws.com:1521/ORCL"
       val prop = new java.util.Properties()
       prop.setProperty("user","ousername")
       prop.setProperty("password","opassword")
       prop.setProperty("driver","oracle.jdbc.OracleDriver")
       res.write.mode(SaveMode.Append).jdbc(url,"nifijson",prop)


    }
    ssc.start()
    ssc.awaitTermination()
  }
}
