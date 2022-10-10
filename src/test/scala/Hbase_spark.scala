import SparkBigData._
import org.apache.spark.sql._               /*important pour dataframe */
import org.apache.spark.sql.types._        /*important pour dataframe */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.hadoop.hbase._


object Hbase_spark {
    def catalog_orders = s"""{
                            |        "table":{"namespace":"default", "name":"table_orders"},
                            |        "rowkey":"key",
                            |        "columns":{
                            |        "order_id":{"cf":"rowkey", "col":"key", "type":"string"},
                            |        "customer_id":{"cf":"orders", "col":"customer_id", "type":"string"},
                            |        "campaign_id":{"cf":"orders", "col":"campaign_id", "type":"string"},
                            |        "order_date":{"cf":"orders", "col":"order_date", "type":"string"},
                            |        "city":{"cf":"orders", "col":"city", "type":"string"},
                            |        "state":{"cf":"orders", "col":"state", "type":"string"}
                            |        }
                            |        }""".stripMargin

  def main(args: Array[String]): Unit = {
    val ss = Session_Spark(true)

    val df_hbase = ss.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog_orders))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

   // df_hbase.printSchema()
   // df_hbase.show(false)

    df_hbase.createOrReplaceTempView("orders")
    //ss.sql("select * from orders where state = 'MA'").show(5)

  }
}
