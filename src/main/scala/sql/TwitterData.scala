package sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object TwitterData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("TwitterData").getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val APIkey="eg7tzWqTZNW8qUE4H8R0Nkbmh"
    val APIkeysecret="bf8WbWM5jipdF5IvUYrW09CiBL6GI99x0OqaGZH92PmPo6LJIn"
    val Accesstoken="1285746016956841990-4r7ZQPQtTliaXfGwaGqnm33nkHo6o9"
    val Accesstokensecret="IQBdDzTZmmbnA6lfip2bDdG9Q4kfRN0DIOPW0yjhlVRim"
    val searchFilter = "Corona, CoronaVirus, covid19"

    val interval = 10

    System.setProperty("twitter4j.oauth.consumerKey", APIkey)
    System.setProperty("twitter4j.oauth.consumerSecret", APIkeysecret)
    System.setProperty("twitter4j.oauth.accessToken", Accesstoken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", Accesstokensecret)
    //val lines = ssc.socketTextStream("localhost", 9999)


    // create dstream
    val tweetStream = TwitterUtils.createStream(ssc, None, Seq(searchFilter.toString))
    tweetStream.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = x.map(x => (x.getText(), x.getUser().getScreenName())).toDF("tweet", "username")
      df.show()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
