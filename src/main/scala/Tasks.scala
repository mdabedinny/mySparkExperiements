import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Tasks {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Tasks").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    val data = "C:\\work\\datasets\\us-500.csv"
    val usrdd = sc.textFile(data)
    val head = usrdd.first()
    //task1:select state, count(*) cnt from table group by state order by cnt desc
    val task1res = usrdd.filter(x=>x!=head).map(x=> x.split(reg)).map(x=>(x(6).replaceAll("\"", ""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    task1res.take(5).foreach(println)
    //task2: et eg: 6 th record  county, and city having same names (Ashland)
    // similarly 18 th line (Milwaukee) find how many names like that
    val task2res = usrdd.filter(x=>x!=head).map(x=> x.split(reg)).map(x=>(x(4).replaceAll("\"", ""),x(5).replaceAll("\"", ""), 1)).filter(x=> x._1==x._2)
    task2res.take(5).foreach(println)

    //task3: Let eg: 4 th line (Art) first_name you have 3 letters in this way how many lines
    // you have with 3 letters length list out that datasets.
    val task3res =usrdd.filter(x=>x!=head).map(x=> x.split(reg)).map(x=>(x(0).replaceAll("\"", ""),x(1).replaceAll("\"", ""),
      x(2).replaceAll("\"", ""),x(3).replaceAll("\"", ""),x(4).replaceAll("\"", ""),x(5).replaceAll("\"", ""),
      x(6).replaceAll("\"", ""),x(7).replaceAll("\"", ""),x(8).replaceAll("\"", ""),x(9).replaceAll("\"", ""),
      x(10).replaceAll("\"", ""),x(11).replaceAll("\"", ""))).filter(x=>x._1.length==3)
     task3res.take(5).foreach(println)

    //task4:select SUBSTRING_INDEX(SUBSTR(email, INSTR(EMail, '@') +1),'.',1) email,count(*) cnt from tab group by SUBSTRING_INDEX(SUBSTR(EMail,
    // INSTR(EMail, '@') +1),'.',1) order by cnt desc

    val task4res =usrdd.filter(x=>x!=head).map(x=> x.split(reg)).map(x=>(x(0).replaceAll("\"", ""),x(1).replaceAll("\"", ""),
      x(2).replaceAll("\"", ""),x(3).replaceAll("\"", ""),x(4).replaceAll("\"", ""),x(5).replaceAll("\"", ""),
      x(6).replaceAll("\"", ""),x(7).replaceAll("\"", ""),x(8).replaceAll("\"", ""),x(9).replaceAll("\"", ""),
      x(10).replaceAll("\"", ""),x(11).replaceAll("\"", ""))).map(x=>(x._11.split("@")(1),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2, false)
    task4res.take(10).foreach(println)

    //Task5: in Phone1 and Phone2 you have - symbol remove that - symbol, next remove space (" ") symbol
    // in City let eg: (New Orleans change to NewOrleans)
    val rx = "[]^a-zA-Z3-9]"
    val task5res =usrdd.filter(x=>x!=head).map(x=> x.split(reg)).map(x=>(x(0).replaceAll("\"", ""),x(1).replaceAll("\"", ""),
      x(2).replaceAll("\"", ""),x(3).replaceAll("\"", ""),x(4).replaceAll(rx, ""),x(5).replaceAll("\"", ""),
      x(6).replaceAll("\"", ""),x(7).replaceAll("\"", ""),x(8).replaceAll(rx, ""),x(9).replaceAll(rx, ""),
      x(10).replaceAll("\"", ""),x(11).replaceAll("\"", "")))
    task5res.take(10).foreach(println)


    spark.stop()
  }
}
