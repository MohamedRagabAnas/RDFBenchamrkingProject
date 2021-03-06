package ee.ut.cs.bigdata.sp2bench.postgresql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SingleStatementTable {

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


    // GRANT ALL PRIVILEGES ON TABLE singlestatementtable TO ragab; // this run in postgres shell to remove to give ragab all privilliges to access this table

    val SingleTableDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "singlestatementtable")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()


    SingleTableDF.createOrReplaceTempView("MyRDFTable")




    //RDFDF.show()

    // spark.sql("SELECT count(*) AS Count from MyRDFTable").show()


    /////////////////////\\\SP2Bench Query1 Single Triple Store\\\/////////////////////
    //val tq1 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT
        |T3.object AS Year
        |FROM MyRDFTable T1, MyRDFTable T2, MyRDFTable T3
        |WHERE T1.subject=T2.subject
        |AND   T2.subject=T3.subject
        |AND T1.object='http://localhost/vocabulary/bench/Journal'
        |AND T2.predicate='http://purl.org/dc/elements/1.1/title'
        |AND T2.object='Journal 1 (1940)'
        |AND T3.predicate='http://purl.org/dc/terms/issued'
        | """.stripMargin).show)

    //val durationQuery1 = (System.nanoTime - tq1) / 1e9d
    //println(durationQuery1)

    /////////////////////\\\SP2Bench Query2 Single Triple Store\\\/////////////////////

    //val tq2 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT
        |T1.subject AS inproc, T2.object AS author, T3.object AS booktitle, T7.object AS title, T4.object AS proc,
        |T5.object AS ee, T6.object AS page, T8.object AS homepage, T9.object AS issued, T10.object AS abstract
        |
        |FROM MyRDFTable T1, MyRDFTable T2, MyRDFTable T3 ,
        |MyRDFTable T7, MyRDFTable T8, MyRDFTable T9
        |
        |LEFT JOIN MyRDFTable T10 ON T10.subject=T1.subject  AND  T10.predicate='http://localhost/vocabulary/bench/abstract'
        |LEFT JOIN MyRDFTable T6 ON T6.subject=T1.subject    AND  T6.predicate= 'http://swrc.ontoware.org/ontology#pages'
        |LEFT JOIN MyRDFTable T5 ON T5.subject=T1.subject    AND  T5.predicate='http://www.w3.org/2000/01/rdf-schema#seeAlso'
        |LEFT JOIN MyRDFTable T4 ON T4.subject=T1.subject    AND  T4.predicate='http://purl.org/dc/terms/partOf'
        |
        |WHERE T1.subject=T2.subject
        |AND   T2.subject=T3.subject
        |AND   T3.subject=T7.subject
        |AND   T7.subject=T8.subject
        |AND   T8.subject=T9.subject
        |
        |AND T1.object='http://localhost/vocabulary/bench/Inproceedings'
        |AND T2.predicate='http://purl.org/dc/elements/1.1/creator'
        |AND T3.predicate='http://localhost/vocabulary/bench/booktitle'
        |AND T7.predicate='http://purl.org/dc/elements/1.1/title'
        |AND T8.predicate='http://xmlns.com/foaf/0.1/homepage'
        |AND T9.predicate='http://purl.org/dc/terms/issued'
        |
        |ORDER BY issued
        | """.stripMargin).show)

    //val durationQuery2 = (System.nanoTime - tq2) / 1e9d
    //println(durationQuery2)


    /////////////////////\\\SP2Bench Query3 Single Triple Store\\\/////////////////////
    //val tq3 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT DISTINCT A1.subject AS article
        |FROM MyRDFTable A1
        |LEFT JOIN MyRDFTable A2 ON A2.subject=A1.subject  AND  A2.predicate= 'http://swrc.ontoware.org/ontology#pages'
        |WHERE
        |A1.object='http://localhost/vocabulary/bench/Article'
        |AND
        |A2.object IS NOT NULL
        | """.stripMargin).show)

    //val durationQuery3 = (System.nanoTime - tq3) / 1e9d
    //println(durationQuery3)



    /////////////////////////////////////////// Q4////////////////////////////
    //val tq4 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT DISTINCT
        |T3.object AS name1, T8.object AS name2
        |FROM
        |    MyRDFTable T1 , MyRDFTable T2 , MyRDFTable T3,
        |    MyRDFTable T4, MyRDFTable T5, MyRDFTable T6, MyRDFTable T7, MyRDFTable T8
        |
        |WHERE
        |      T1.subject=T2.subject
        |AND   T2.object=T3.subject
        |AND   T1.subject=T4.subject
        |AND   T4.object=T5.object
        |AND   T5.subject=T6.subject
        |AND   T6.subject=T7.subject
        |AND   T7.object=T8.subject
        |
        |
        |AND
        |T1.object='http://localhost/vocabulary/bench/Article'
        |AND T6.object='http://localhost/vocabulary/bench/Article'
        |
        |AND T2.predicate='http://purl.org/dc/elements/1.1/creator'
        |AND T7.predicate='http://purl.org/dc/elements/1.1/creator'
        |
        |AND T3.predicate='http://xmlns.com/foaf/0.1/name'
        |AND T8.predicate='http://xmlns.com/foaf/0.1/name'
        |
        |AND T4.predicate='http://swrc.ontoware.org/ontology#journal'
        |AND T5.predicate='http://swrc.ontoware.org/ontology#journal'
        |
        |AND T3.object<T8.object
      """.stripMargin ).show())

    //val durationQuery4 = (System.nanoTime - tq4) / 1e9d
    //println(durationQuery4)


    //////////////////////////////////// Query 5  Single Statement Table/////////////////////



    spark.time(spark.sql(
      """
        |SELECT DISTINCT
        |T3.subject AS person, T6.object AS name
        |FROM
        |    MyRDFTable T1 , MyRDFTable T2 , MyRDFTable T3,
        |    MyRDFTable T4, MyRDFTable T5, MyRDFTable T6
        |
        |WHERE
        |      T1.subject=T2.subject
        |AND   T2.object=T3.subject
        |
        |AND   T4.subject=T5.subject
        |AND   T5.object=T6.subject
        |AND
        |T1.object='http://localhost/vocabulary/bench/Article'
        |AND T4.object='http://localhost/vocabulary/bench/Inproceedings'
        |
        |AND T2.predicate='http://purl.org/dc/elements/1.1/creator'
        |AND T5.predicate='http://purl.org/dc/elements/1.1/creator'
        |
        |AND T3.predicate='http://xmlns.com/foaf/0.1/name'
        |AND T6.predicate='http://xmlns.com/foaf/0.1/name'
        |AND T3.object=T6.object
      """.stripMargin ).show())






    //////////////////////////////////// Query 6  Single Statement Table/////////////////////

    spark.time(spark.sql(
      """
      SELECT
     |    L1.yr       AS yr,
     |    L1.name     AS name,
     |    L1.document AS document
     |FROM
     |    (
     |        SELECT
     |            T1.subject    AS class,
     |            T2.subject    AS document,
     |            T3.object     AS yr,
     |            T4.object     AS author,
     |            T5.object     AS name
     |        FROM
     |            MyRDFTable T1
     |
     |            JOIN MyRDFTable T2     ON T1.subject=T2.object
     |
     |            JOIN MyRDFTable T3     ON T3.subject=T2.subject
     |
     |            JOIN MyRDFTable T4     ON T4.subject=T3.subject
     |
     |            JOIN MyRDFTable T5     ON T5.subject=T4.object
     |
     |        WHERE
     |                T1.predicate='http://www.w3.org/2000/01/rdf-schema#subClassOf'
     |            AND T2.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
     |            AND T3.predicate='http://purl.org/dc/terms/issued'
     |            AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
     |            AND T5.predicate='http://xmlns.com/foaf/0.1/name'
     |            AND T1.object='http://xmlns.com/foaf/0.1/Document'
     |    ) L1
     |
     |    LEFT JOIN
     |    (
     |        SELECT
     |            T1.subject    AS class,
     |            T2.subject    AS document,
     |            T3.object     AS yr,
     |            T4.object     AS author
     |        FROM
     |            MyRDFTable T1
     |
     |            JOIN MyRDFTable T2     ON T1.subject=T2.object
     |
     |            JOIN MyRDFTable T3     ON T3.subject=T2.subject
     |
     |            JOIN MyRDFTable T4     ON T4.subject=T3.subject
     |
     |        WHERE
     |                T1.predicate='http://www.w3.org/2000/01/rdf-schema#subClassOf'
     |            AND T2.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
     |            AND T3.predicate='http://purl.org/dc/terms/issued'
     |            AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
     |            AND T1.object='http://xmlns.com/foaf/0.1/Document'
     |    ) L2
     |    ON L1.author=L2.author AND L2.yr<L1.yr
     |WHERE L2.author IS NULL
      """.stripMargin ).show())






    //////////////////////////////////// Query 8  Single Statement Table/////////////////////



    spark.time(spark.sql(
      """
        |SELECT DISTINCT
        |    name
        |FROM
        |    MyRDFTable T1
        |    JOIN MyRDFTable T2     ON T1.subject=T2.subject
        |    JOIN
        |    (
        |        SELECT
        |            name,
        |            erdoes
        |        FROM
        |        (
        |            SELECT
        |                T5.object     AS name,
        |                T3.object    AS erdoes
        |            FROM
        |                MyRDFTable T3
        |                JOIN MyRDFTable T4     ON T3.subject=T4.subject
        |                JOIN MyRDFTable T5     ON T4.object=T5.subject
        |
        |            WHERE
        |                    T3.predicate='http://purl.org/dc/elements/1.1/creator'
        |                AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
        |                AND T5.predicate='http://xmlns.com/foaf/0.1/name'
        |                AND NOT T3.object=T4.object
        |        ) L
        |        UNION
        |        (
        |            SELECT
        |                T7.object AS name,
        |                T3.object AS erdoes
        |            FROM
        |                MyRDFTable T3
        |                JOIN MyRDFTable T4     ON T3.subject=T4.subject
        |                JOIN MyRDFTable T5     ON T4.object=T5.object
        |                JOIN MyRDFTable T6     ON T5.subject=T6.subject
        |                JOIN MyRDFTable T7     ON T6.object=T7.subject
        |
        |            WHERE
        |                T3.predicate='http://purl.org/dc/elements/1.1/creator'
        |                AND T4.predicate='http://purl.org/dc/elements/1.1/creator'
        |                AND T5.predicate='http://purl.org/dc/elements/1.1/creator'
        |                AND T6.predicate='http://purl.org/dc/elements/1.1/creator'
        |                AND T7.predicate='http://xmlns.com/foaf/0.1/name'
        |
        |
        |                AND NOT T4.object=T3.object
        |                AND NOT T5.subject=T3.subject
        |                AND NOT T6.object=T3.object
        |                AND NOT T4.object=T6.object
        |        )
        |    ) R ON T2.subject=R.erdoes
        |WHERE
        |        T1.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        |    AND T2.predicate='http://xmlns.com/foaf/0.1/name'
        |    AND T1.object='http://xmlns.com/foaf/0.1/Person'
        |    AND T2.object='Paul Erdoes'
      """.stripMargin).show()
    )



    ///////////////////////////////////////// Q9 ////////////////////////////////////////////



    spark.time(spark.sql(
      """
        |SELECT DISTINCT L.predicate AS predicate
        |FROM
        |    (
        |        SELECT
        |            T1.subject  AS subject,
        |            T2.predicate AS predicate
        |        FROM
        |            MyRDFTable T1
        |            JOIN MyRDFTable T2     ON T1.subject=T2.object
        |
        |        WHERE
        |                T1.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        |            AND T1.object='http://xmlns.com/foaf/0.1/Person'
        |        UNION
        |        SELECT
        |            T1.subject   AS subject,
        |            T2.predicate AS predicate
        |        FROM
        |            MyRDFTable T1
        |            JOIN MyRDFTable T2     ON T1.subject=T2.subject
        |        WHERE
        |                T1.predicate='http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
        |            AND T1.object='http://xmlns.com/foaf/0.1/Person'
        |    ) L
      """.stripMargin).show())





    ////////////////////////////////// query 10 ////////////////////////


    spark.time(spark.sql(
      """
        |SELECT
        |    T.subject  AS subject,
        |    T.predicate AS predicate
        |FROM
        |    MyRDFTable T
        |WHERE
        |    T.object='http://localhost/persons/Paul_Erdoes'
      """.stripMargin).show())



    /////////////////////////////////////query 11/////////////////////////////////////////////
    spark.time(spark.sql(
      """
        |SELECT
        |T1.object AS ee
        |FROM MyRDFTable T1
        |WHERE T1.predicate='http://www.w3.org/2000/01/rdf-schema#seeAlso'
        |ORDER BY ee DESC
        |LIMIT 10
        |
        | """.stripMargin).show)









  }

}