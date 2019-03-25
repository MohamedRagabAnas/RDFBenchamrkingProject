package ee.ut.cs.bigdata.sp2bench.postgresql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object VerticalTables {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")


    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionZipsExample")
      .getOrCreate()


    val RDFDFTitle = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "title")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()

    RDFDFTitle.createOrReplaceTempView("Title")


    val RDFDFIssued = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "issued")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()

    RDFDFIssued.createOrReplaceTempView("Issued")


    val RDFDFType = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "type")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFType.createOrReplaceTempView("Type")


    val RDFDFCreator = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "creator")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFCreator.createOrReplaceTempView("Creator")


    val RDFDFBookTitle = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "booktitle")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFBookTitle.createOrReplaceTempView("BookTitle")

    val RDFDFPartOf = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "partof")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFPartOf.createOrReplaceTempView("PartOf")


    val RDFDFSeeAlso = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "seealso")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFSeeAlso.createOrReplaceTempView("SeeAlso")

    val RDFDFPages = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "pages")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFPages.createOrReplaceTempView("Pages")


    val RDFDFHomePage = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "homepage")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFHomePage.createOrReplaceTempView("HomePage")


    val RDFDFAbstract = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "abstract")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFAbstract.createOrReplaceTempView("Abstract")


    val RDFDFName = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "name")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFName.createOrReplaceTempView("Name")


    val RDFDFJournal = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "injournal")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()
    RDFDFJournal.createOrReplaceTempView("Journal")


    /////////////////////\\\SP2Bench Query1 VP\\\/////////////////////
    //val tq1 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT
        |T2.object AS Year
        |FROM Title T1, Issued T2, Type T3
        |WHERE T1.subject=T2.subject
        |AND   T2.subject=T3.subject
        |AND T3.object='http://localhost/vocabulary/bench/Journal'
        |AND T1.object='Journal 1 (1940)'
        | """.stripMargin).show)


    // val durationQuery1 = (System.nanoTime - tq1) / 1e9d
    // println(durationQuery1)


    /////////////////////\\\SP2Bench Query2 VP\\\/////////////////////


    //val tq2 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT T1.subject AS inproc, T2.object AS author, T3.object AS booktitle , T4.object AS title,
        |T5.object AS homepage, T6.object AS issued,T8.object as Pages,T9.object as ee, T10.object as proc, T7.object AS abstract
        |
        |FROM Type T1, Creator T2, BookTitle T3, Title T4, HomePage T5, Issued T6
        |LEFT JOIN Abstract T7  ON T7.subject=T6.subject
        |LEFT JOIN Pages T8     ON T8.subject=T7.subject
        |LEFT JOIN SeeAlso T9   ON T9.subject=T8.subject
        |LEFT JOIN PartOf T10   ON T10.subject=T9.subject
        |
        |WHERE T1.object='http://localhost/vocabulary/bench/Inproceedings'
        |AND   T2.subject=T1.subject
        |AND   T3.subject=T2.subject
        |AND   T4.subject=T3.subject
        |AND   T5.subject=T4.subject
        |AND   T6.subject=T5.subject
        |

        |
        |ORDER BY issued
        | """.stripMargin).show())

    // val durationQuery2 = (System.nanoTime - tq2) / 1e9d
    //println(durationQuery2)


    /*println(spark.sql(
    """
      |SELECT T1.subject AS inproc, T2.object AS author, T3.object AS booktitle , T4.object AS booktitle,
      |T5.object AS homepage, T6.object AS issued,T8.object as Pages,T9.object as ee, T10.object as proc, T7.object AS abstract
      |
      |FROM Type T1 left join  Creator T2 on T1.subject=T2.subject
      |
      |LEFT join BookTitle T3 on T1.subject=T3.subject
      |LEFT join Title T4 on T1.subject=T4.subject
      |LEFT join HomePage T5 on T1.subject=T5.subject
      |LEFT join Issued T6 on T1.subject=T6.subject
      |LEFT JOIN Pages T8     ON T1.subject=T8.subject
      |LEFT JOIN SeeAlso T9   ON T1.subject=T9.subject
      |LEFT JOIN PartOf T10   ON T1.subject=T10.subject
      |LEFT JOIN Abstract T7  ON T1.subject=T7.subject
      |
      |WHERE T1.object='http://localhost/vocabulary/bench/Inproceedings'
      |
      |ORDER BY issued
      | """.stripMargin).count)*/


    /////////////////////\\\SP2Bench Query3 Vertical Tables\\\/////////////////////
    //val tq3 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT DISTINCT Ty.subject AS article
        |
        |FROM Type Ty
        |JOIN Pages P ON Ty.subject=P.subject
        |WHERE
        |Ty.object='http://localhost/vocabulary/bench/Article'
        |AND
        |P.object IS NOT NULL
        | """.stripMargin).show())

    //val durationQuery3 = (System.nanoTime - tq3) / 1e9d
    //println(durationQuery3)


    /////////////////////////Sp2Bench Q4 Vertical Tables///////////

    //val tq4 = System.nanoTime


    spark.time(spark.sql(
      """
        |SELECT DISTINCT
        |    N1.object AS name1,
        |    N2.object AS name2
        |FROM
        |    Type A1
        |
        |    JOIN Creator C1 ON A1.subject=C1.subject
        |    JOIN Name N1    ON C1.object=N1.subject
        |    JOIN Journal J1 ON A1.subject=J1.subject
        |    JOIN Journal J2 ON J1.object=J2.object
        |    JOIN Type A2    ON A2.subject=J2.subject
        |    JOIN Creator C2 ON A2.subject=C2.subject
        |    JOIN Name N2    ON C2.object=N2.subject
        |WHERE
        |    A1.object='http://localhost/vocabulary/bench/Article'
        |    AND
        |    A2.object='http://localhost/vocabulary/bench/Article'
        |    AND N1.object<>N2.object
      """.stripMargin

    ).show())

    //val durationQuery4 = (System.nanoTime - tq4) / 1e9d
    //println(durationQuery4)


    /////////////////////////Sp2Bench Q5 Vertical Tables///////////

    spark.time(spark.sql(
      """
        |SELECT DISTINCT
        |    N1.subject AS person,
        |    N1.object AS name
        |FROM
        |    Type P1
        |    JOIN Creator C1 ON P1.subject=C1.subject
        |    JOIN Name N1    ON C1.object=N1.subject,
        |    Type P2
        |    JOIN Creator C2 ON P2.subject=C2.subject
        |    JOIN Name N2    ON C2.object=N2.subject
        |WHERE
        |    P1.object='http://localhost/vocabulary/bench/Article'  AND
        |    P2.object='http://localhost/vocabulary/bench/Inproceedings'
        |    AND N1.object=N2.object
      """.stripMargin

    ).show())


  }
}