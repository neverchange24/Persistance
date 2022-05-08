import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._               /*important pour dataframe */
import org.apache.spark.sql.types._        /*important pour dataframe */
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.functions._    /*important pour dataframe */
import org.apache.spark.sql.catalyst.plans._    /*important pour jointure dataframe */
import org.apache.spark.storage.StorageLevel


/*import java.util.logging.LogManager
import scala.tools.nsc.interactive.Logger*/

object SparkBigData {
  var ss: SparkSession = null
  var spConf: SparkConf = null

  val schema_order = StructType(Array(
    StructField("orderid", IntegerType, false),
    StructField("customerid", IntegerType, false),
    StructField("compaignid", IntegerType, true),
    StructField("orderdate", TimestampType, true),
    StructField("state", StringType, true),
    StructField("zipcode", StringType, true),
    StructField("paymenttype", StringType, true),
    StructField("totalprice", StringType, true),
    StructField("city", StringType, true),
    //StructField("city",StringType, true),
    StructField("numunit", DoubleType, true),
    StructField("numorderlines", IntegerType, true)
  ))

  /*private var trace_log: Logger = LogManager.getLogger("Logger_Console") */
  def main(args: Array[String]): Unit = {
    val session_s = Session_Spark(true)

    val def_test = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\2010-12-06.csv")

    val def_gp = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\csvs\\")
    //def_gp.show(7)

    val def_gp2 = session_s.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\2010-12-06.csv", "C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\2011-12-08.csv")

    //def_gp2.show(7)
    // println("def_gp count :" + def_gp.count() + " def_gp2 count : " + def_gp2.count())
    //def_test.printSchema()

    val df_col = def_test.select(
      col("InvoiceNo").cast(StringType),
      col("_c0").alias("ID Client"),
      col("StockCode").cast(IntegerType).alias("Code Stock"),
      col("Invoice".concat("No")).alias("ID Commande"))

    //df_col.show(10)

    /*Jointure dataframe */
    val df_order = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .schema(schema_order)
      .load("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\orders.txt")
    //df_order.printSchema()
    //df_order.show(5)

    val def_oderok = df_order.withColumnRenamed("numunits", "numunits_order")
      .withColumnRenamed("totalprice", "totalprice_order")


    //df_order.printSchema()
    //df_order.show(15)
    //df_order.printSchema()

    val df_prod = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\product.txt")

    //df_order.printSchema()
    //df_prod.show(10)
    //df_prod.printSchema()

    val df_ordel = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .load("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\orderline.txt")

    //df_order.printSchema()
    //df_ordel.show(10)
    //df_ordel.printSchema()

    val df_joined = df_ordel.join(def_oderok, def_oderok.col("orderid") === df_ordel.col("orderid"), "inner")
      .join(df_prod, df_prod.col("productid") === df_ordel.col("productid"), Inner.sql)
    //.show(5)
    // faire l'union des fichiers

    val def_fich1 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\2010-12-06.csv")

    val def_fich2 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\2011-01-20.csv")

    val def_fich3 = session_s.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .csv("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\2011-12-08.csv")

    val df_union = def_fich1.union(def_fich2.union(def_fich3))
    //println(df_fich.count()+ " "+ df_union.count())
    //df_union.show(10)

    df_joined.withColumn("Total_amounts", round(col("numunits") * col("totalprice"), 3)).alias("city_sum")
      .groupBy(col1 = "state")
      .sum("Total_amounts")
    //.show(10)

    val wr_spec = org.apache.spark.sql.expressions.Window.partitionBy(col("state"))

    val df_window = df_joined.withColumn("Ventes_dep", sum(round(col("numunits") * col("totalprice"), 3)).over(wr_spec))
      .select(
        col("PRODUCTGROUPCODE"),
        col("zipcode"),
        col("PRODUCTGROUPNAME"),
        col("state"),
        col("Ventes_dep").alias("Ventes_par_departement")
      )
      //.show(10)

        df_window.repartition(1)
          .write
          .format("com.databricks.spark.csv")
         .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\DataFrame\\Ecriture")
  }


//desc_nulls_first()

  def mainip_rdd() : Unit = {
    val session_s = Session_Spark(true)
    val sc = session_s.sparkContext

    sc.setLogLevel("OFF")

    val rdd_test: RDD[String] = sc.parallelize(List("sosi", "juve", "anne", "diego"))
    rdd_test.foreach {
      l => println(l)
    }
    println()
    val rdd2: RDD[String] = sc.parallelize(Array("never", "dany", "anny", "dadi"))
    rdd2.foreach { l => println(l) }

    println()

    val rdd3 = sc.parallelize(Seq(("JEAN", "Dido", 15), ("Maria", "Didier", 25), ("Anto", "Eric", 35)))
    println("Premiers éléments de mon RDD3")
    rdd3.take(2).foreach { l => println(l) }
    if(rdd3.isEmpty()){
      println("Le rdd3 est vide")
    } else{
      rdd3.foreach{ l=> println(l)}
    }
    rdd3.saveAsTextFile("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\rdd3.txt")
    //rdd3.repartition(1).saveAsTextFile("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\rdd0.txt")
    //rdd3.foreach{l=>println(l)}
    //rdd3.collect().foreach{l=>println(l)}

    // creation d'un RDD à partir des sources de données
    val rdd4 = sc.textFile("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\textRDD\\rddnew.txt")
    //println("Lecture du contenu du RDD4:")
    //rdd4.foreach{l=>println(l)}

    val rdd5 = sc.textFile("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source\\textRDD\\*")
    //println("Lecture du contenu du RDD5:")
    //rdd5.foreach{t=>println(t)}

    //transformations des RDD
    val rdd_trans: RDD[String] = sc.parallelize(List("Alain mange la banane", "la banane avec avocat", "jus de fanta"))
    //rdd_trans.foreach(l => println("Ligne de mon RDD: " + l))

    val rdd_map = rdd_trans.map(x => x.split(""))
    //println("Nombre d'élements map: " + rdd_map.count())

    val rdd6 = rdd_trans.map(w => (w,w.length, w.contains("banane")))
    val rdd7 = rdd6.map(x=> (x._1.toUpperCase(),x._2,x._3))

    val rdd8 = rdd6.map(x=> (x._1. split(" "),1))
    //rdd8.foreach(l => println(l._1(0),l._2))
    val rdd_fm = rdd_trans.flatMap(x=> x.split(" ")).map(w =>(w,1))
    //rdd_fm.foreach(l=> println(l))
    val rdd_cmp = rdd5.flatMap(x=> x.split(",")).flatMap(x=> x.split(" ")).map(w=>(w,1)).reduceByKey((x,y)=> x+y)
    //rdd_cmp.repartition(1).saveAsTextFile("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\sos1.txt")
    //rdd_cmp.foreach(l=> println(l))

    /* transformation avec filter */
    val rdd_fil = rdd_fm.filter(x=> x._1.startsWith("m"))
    val rdd_filter = rdd_fm.filter(x => x._1.contains("banane"))
    /* transformation avec mapreduce */
    val rdd_reduced = rdd_fm.reduceByKey((x,y)=> x+y)
    rdd_fm.cache()
    //rdd_fm.persist(StorageLevel.MEMORY_AND_DISK)
    //rdd_fm.unpersist()

    import session_s.implicits._
    val df: DataFrame = rdd_fm.toDF("texte", "valeur")
    //df.show(30)


    //rdd_reduced.foreach(l=> println(l))
  }



  /**
   * fonction qui initialise et instancie une session spark
   *
   * @param Env : une variable qui indique sur l'environnement sur lequel on est déployé.
   *            si true= l'environnement est déployé en local sinon en production
   */

  def Session_Spark(Env: Boolean = true): SparkSession = {
      if (Env == true) {
        System.setProperty("hadood.home.dir", "C:/Hadoop")
        ss = SparkSession.builder()
          .master("local[*]")
          .config("spark.sql.crossJoin.enabled", "true")
          /*.enableHiveSupport()*/
          .getOrCreate()

      } else {
        ss = SparkSession.builder()
          .appName("Application Spark")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.crossJoin.enabled", "true")
          /*.enableHiveSupport()*/
          .getOrCreate()
      }
     /*catch {
      case ex: java.io.FileNotFoundException => trace_log.error("Nous n'avons pas trouvé winutils dans le schéma indiqué" + ex.printStacktrace())
      case ex: Exception => trace_log.error("Erreur dans l'initialisation de la session Spark" + ex.printStackTrace())
    }*/
    return ss
  }
}