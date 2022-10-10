import org.apache.log4j._
import scala.collection.mutable._

  object HelloBigData {
    /* premier programme en scala */

    val ma_var_imm : String = "Sosthène"  /* variable immutable */
    private val une_var_imm : String = "Formation Big Data"  /* variable à portée privée */

    class personne (var nom: String, var prenom : String, var age: Int)
    /*BasicConfigurator.configure()*/
    private var trace_appli :Logger = LogManager.getLogger("Logger_console")


    def main(args: Array[String]): Unit = {
      println("Hello word: mon premier programme en scala")
      var var_mut : Int = 15
      var_mut = var_mut + 19
      println(var_mut)

      val test_imm : Int = 17

      println("votre texte contient: "+ comptage_mots(texte = "qu'avez-vous vu ce matin ?  ")+ " caractères")
      getResultat(parametre = 10)
      testwhile(valeur_cond = 10)
      testfor()
      collectionScala()
      collectiontuple()

    }

    /* Ma première fonction: renvoie un résultats */
    def Comptage_mots (texte : String) : Int = {
      trace_appli.info("demarrage de tracage de la classe")
      trace_appli.info(s"le paramètre tracé du log4j pour cette fonction est: $texte")
      trace_appli.warn(s"Message d'avertissement du log4j interpolation de chaines : ${10+15}")
    if (texte.isEmpty) {
    0
    } else{
      texte.trim.length()
    }
    }

    /* Syntaxe2 */
    def comptage_mots (texte : String) : Int = {

      return texte.trim.length()
    }

    /* Syntaxe3
    def comptage_mots (texte : String) : Int = return  texte.trim.length() */

    /* exemple de procédure: renvoie une action */
    def getResultat (parametre: Any): Unit ={
      if (parametre == 10) {
        print("votre valeur est un entier")
      } else println("votre valeur est une chaine de caractère")

    }
    /* tester de la condition while */
    def testwhile(valeur_cond: Int) : Unit ={
      var i: Int = 0
      while (i < valeur_cond) {
        println ("Iteration N° " + i)
        i = i+1
      }
    }
    /* tester de la condition while */
    def testfor(): Unit ={
      var j: Int = 0
      for (j <- 5 to 15) {
        println ("Iteration N° " + j)
      }
    }
    // les collections en scala */
    def collectionScala() : Unit= {
      val maliste: List[Int] = List(1, 2, 5, 8, 3, 6)
      val machaine: List[String] = List("Paul", "Vio", "theo", "Jean")
      val maplage_v: List[Int] = List.range(2, 15, 3)

      println(maliste(2))
      println(machaine(2))

      for (i <- machaine) {
        println(i)
      }
      // manipulation des donnnées en collection */
      val resultat: List[String] = machaine.filter(f => f.endsWith("0"))
      for (r <- resultat) {
        println(r)
      }
      val res: Int = machaine.count(e => e.endsWith("o"))
      println("Nombre d'éléments respectant la condition:  " + res)

      val res1: List[Int] = maliste.map(i => i * 2)
      for (r <- res1) {
        println(r)
      }
      /*val res2: List[Int] = maliste.map((e:Int) => e*2)) */
      val res3: List[Int] = maliste.map(_ * 3)
      for (l <- res3) {
        println(l)
      }
      val nou_liste: List[Int] = maplage_v.filter(p => p > 5)
      val new_liste: List[String] = machaine.map(s => s.capitalize)

      new_liste.foreach(e => println("Nouvelle liste: " + e))
      nou_liste.foreach(e => println("Nouvelle liste: " + e))
      maplage_v.foreach(println(_))
    }

    def collectiontuple(): Unit = {
      val tuple_test = ("sos", "ddo", "45")
      println(tuple_test._2)
    }
    val new_personne: personne= new personne ( "IDI", "SOSTHENE", 43)
    val tuple_2 = ("test", new_personne, 65)
    tuple_2.toString().toList

    /* Valeur map      */
    val states = Map(
      "Al"-> "Alska",
      "Am" -> "Amerique",
      "Bu" -> "Burundi"
    )
    val persona = Map(
      "Nom"-> "Idi",
      "Prenom"->"Sosthène",
      "Age" -> 52
    )
    persona.foreach(e => println(e))
    /* les tableaux  */
    val montableau: Array[String]= Array("soso", "dodo", "testo")
    montableau.foreach(u => println(u))
  }


