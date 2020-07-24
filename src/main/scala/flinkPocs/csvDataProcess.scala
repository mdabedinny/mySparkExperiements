package flinkPocs

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
//import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
//import org.apache.flink.table.api.bridge.scala._
object csvDataProcess {
  case class aslcc(name:String, age:Int, city:String)
  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment // if ur using batch processing use this
  //  val env = StreamExecutionEnvironment.getExecutionEnvironment() // if ur using live data use this
  //val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    val data = "C:\\work\\datasets\\datasets\\asl.txt"
    val output = "C:\\work\\datasets\\output\\flinkop\\aslblr.csv"
        val ds = env.readCsvFile[aslcc](data,ignoreFirstLine = true)
    //convert dataset to tableapi
    val tab = tEnv.fromDataSet(ds)
    // how ur using createOrReplacetempview same way register a table
    tEnv.registerTable("asl", tab)
    // run sql query
    // spark.sql("select * from tab")
    val res = tEnv.sqlQuery("select * from asl where city='Bangalore'")

    // table api convert to Dataset
    val fds = tEnv.toDataSet[aslcc](res)

    fds.writeAsCsv(output).setParallelism(1)
    fds.print()


  }
}
