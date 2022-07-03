

import SparkBigData._
import org.apache.spark.sql._               /*important pour dataframe */
import org.apache.spark.sql.types._        /*important pour dataframe */
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.functions._    /*important pour dataframe */
import org.apache.spark.sql.catalyst.plans._    /*important pour jointure dataframe */
import java.util._



object Spark_DB {
  def main(args: Array[String]): Unit = {
    val ss = Session_Spark(true)

    val prop_mysql = new Properties()
    prop_mysql.put("user","consultant")
    prop_mysql.put("password","pwd#86")

    val df_mysql = ss.read.jdbc("jdbc:127.0.0.1:3306/jd_bb","jd_bb.orders",prop_mysql)

    val df_mysql2 = ss.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306")
      .option("user", "consultant")
      .option("password", "pwd#86")  //
      .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jd_bb.orders group by state, city) table_summary")
      .load()

    df_mysql2.show(10)
  }
}
