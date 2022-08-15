import SparkBigData._
import org.apache.spark.sql._ /*important pour dataframe */
import org.apache.spark.sql.types._ /*important pour dataframe */
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

object UseCaseBano {
  val schema_bano = StructType(Array(
    StructField("id_bano", StringType, false),
    StructField("numero_voie", StringType, false),
    StructField("nom_voie", StringType, false),
    StructField("code_postal", StringType, false),
    StructField("nom_commune", StringType, false),
    StructField("code_source_bano", StringType, false),
    StructField("latitude", StringType, true),
    StructField("longitude", StringType, true)
  ))
  val confighd = new Configuration()
  val fs = FileSystem.get(confighd)

  val chemin_dest = new Path("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\Source/Now")

  def main(arg: Array[String]) : Unit ={
    val ss = Session_Spark(true)
    val df_bano_brut = ss.read
      .format("com.databrics.spark.csv")
      .option("delimiter",",")
      .option("header","true")
      .schema(schema_bano)
      .csv("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\full.csv")
    //df_bano_brut.show(10)
    val df_bano = df_bano_brut
      .withColumn("code_departement", substring(col("code_postal"),1,2))
      .withColumn("libelle_source", when(col("code_source_bano")=== lit("OSM"),lit("OpenStreeMap"))
        .otherwise(when(col("code_source_bano")=== lit("OD"),lit("OpenData"))
          otherwise(when(col("code_source_bano")=== lit("O+O"),lit("OpenData OSM"))
          .otherwise(when(col("code_source_bano")=== lit("CAD"),lit("Cadastre"))
            .otherwise(when(col("code_source_bano")=== lit("C+O"),lit("Cadastre OSM")))))))
    //df_bano.show(10)

    val df_departement = df_bano.select(col("code_departement")).distinct().filter(col("code_departement").isNotNull)

    val list_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)
      .collect()
      .map(x =>x(0)).toList

    //df_departement.show()

    //list_departement.foreach(e =>println(e.toString))

    list_departement.foreach{
      x => df_bano.filter(col("code_departement")=== x.toString)
        .coalesce(1)
        .write
        .format(source="com.databrics.spark.csv")
        .option("delimiter",";")
        .option("header","true")
        .mode(SaveMode.Overwrite)
        .csv("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\bano"+ x.toString)

        val chemin_source = new Path("C:\\Users\\Alex\\Documents\\Fichier 2021\\FomaBigData\\bano" + x.toString)
        fs.copyFromLocalFile(chemin_source,chemin_dest)
    }
    /*
    list_departement.foreach{
    dep => df_bano.filter(col("code_departement")=== dep.toString())
    .repartition(1)
    .write
    .format(source="com.databrics.spark.csv")
    .option("delimiter",";")
    .option("header","true")
    .mode(SaveMode.Overwrite)
    .csv("C:\Users\Alex\Documents\Fichier 2021\FomaBigData\bano"+ dep.String )
    val chemin_source = new Path("C:\Users\Alex\Documents\Fichier 2021\FomaBigData\bano" + dep.toString)
    fs.copyFromLocalFile(chemin_source,chemin_dest)
    }*/

  }
  //df_departement.show()
}
