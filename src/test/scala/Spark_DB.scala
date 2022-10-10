
import SparkBigData._
import org.apache.spark.sql._               /*important pour dataframe */
import org.apache.spark.sql.types._        /*important pour dataframe */
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.functions._    /*important pour dataframe */
import org.apache.spark.sql.catalyst.plans._    /*important pour jointure dataframe */
import java.util._
import org.apache.spark.sql.execution.datasources._


object Spark_DB {
  def main(args: Array[String]): Unit = {
    val ss = Session_Spark(true)

    val prop_mysql = new Properties()
    prop_mysql.put("user","consultant")
    prop_mysql.put("password","pwd#86")

    val prop_sqlserver = new Properties()
    prop_sqlserver.put("user","consultant")
    prop_sqlserver.put("password","pwd#86")

    val df_mysql = ss.read.jdbc("jdbc:mysql://127.0.0.1:3306/jd_bb","jd_bb.orders",prop_mysql)
    //df_mysql.show(15)

    val df_mysql2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306")
      .option("user", "consultant")
      .option("password", "pwd#86")  //
      .option("dbtable", "(select compaignid, state, sum(round(numunits * totalprice)) as commandes_totales from jd_bb.orders group by state) table_summary")
      .load()

    //df_mysql2.show(17)

    //val df_sqlserver = ss.read.jdbc("jdbc:sqlserver:// DESKTOP-5Q5TJ5C\\SQLEXPRESS:1433; databaseName=jd_bb","orders",prop_sqlserver)

    val df_sqlserver = ss.read
      .format("jdbc")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver:// DESKTOP-5Q5TJ5C\\\\SQLEXPRESS:11428; databaseName=jd_bb;integrationSecurity=true")
      .option("dbtable", "dbo.orders")
      .load()

    df_sqlserver.show(17)

    val df_pstsql = ss.read
      .format("jdbc")
      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver:// DESKTOP-5Q5TJ5C\\SQLEXPRESS:11428; databaseName=jd_bb;integrationSecurity=true")
      .option("dbtable", "orders")
      .load()

    //df_pstsql.show(17)
  }
}
