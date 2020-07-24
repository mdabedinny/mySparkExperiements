package flinkPocs
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
//import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
//import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.api.java.typeutils.RowTypeInfo

object getMysqlData {
  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment // if ur using batch processing use this
    //  val env = StreamExecutionEnvironment.getExecutionEnvironment() // if ur using live data use this
    val tEnv = BatchTableEnvironment.create(env)
    val fileds_type: Array[TypeInformation[_]] = Array[TypeInformation[_]](
      // ur getting data from mysql so in mysql u have
      BasicTypeInfo.BIG_DEC_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO

    )
    //获取行信息
    val rowTypeInfo: RowTypeInfo = new RowTypeInfo(fileds_type: _*)


    //获取flink-jdbc的InputFormat输入格式
    val jdbcInputFormat: JDBCInputFormat = JDBCInputFormat
      .buildJDBCInputFormat()
      .setDrivername("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .setDBUrl("jdbc:sqlserver://mdabdenmssql.ck6vgv6qae1n.us-east-2.rds.amazonaws.com:1433;databaseName=rafidb")
      .setUsername("msuername")
      .setPassword("mspassword")
      .setQuery("select * from DEPT")
      .setRowTypeInfo(rowTypeInfo)
      .finish()

    //获取jdbc-inputformat中的数据
    val res = env.createInput(jdbcInputFormat)
    //持久化
    res.print()

    //触发执行
   // env.execute("flink jdbc inputformat-")
  }
}
