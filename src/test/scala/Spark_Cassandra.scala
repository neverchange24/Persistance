import SparkBigData._
import org.apache.spark.sql._               /*important pour dataframe */
import org.apache.spark.sql.types._        /*important pour dataframe */
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.cassandra._
//import com.datastax.spark.connector._
//import com.datastax.spark.connector.cql.CassandraConnector


object Spark_Cassandra {
  def main(args: Array[String]): Unit = {
    val ss = Session_Spark(true)


    ss.conf.set(s"ss.sql.catalog.sosi","com.datastax.spark.connector.datasource.CassandraConnector")




  }
}
