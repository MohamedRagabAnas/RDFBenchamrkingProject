package ee.ut.cs.bigdata.sp2bench.hive

import org.apache.spark.sql.SparkSession

object SingleStatementTable {
  def main(args: Array[String]): Unit = {

/*
    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")
*/
  val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSessionZipsExample")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris","thrift://quickstart.cloudera:9083")
      .enableHiveSupport()
      .getOrCreate()


    //val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/SingleStatementTables/singleStatementTable100.csv").toDF()
    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/SingleTable/SingleTable.csv").toDF()
    RDFDF.createOrReplaceTempView("MyRDFTable")

    spark.sql("create table if not exists SingleTableHive as select * from MyRDFTable");

    println("********************SUCCESS********************")

    val resultsHiveDFmyTable = spark.sql("SELECT * FROM SingleTableHive ")


    resultsHiveDFmyTable.show(10)

    println("####################SUCCESS######################")


    //RDFDF.show()

    // spark.sql("SELECT count(*) AS Count from MyRDFTable").show()


    /////////////////////\\\SP2Bench Query1 Single Triple Store\\\/////////////////////
    //val tq1 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT
        |T3.object AS Year
        |FROM SingleTableHive T1, SingleTableHive T2, SingleTableHive T3
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
        |FROM SingleTableHive T1, SingleTableHive T2, SingleTableHive T3 ,
        |SingleTableHive T7, SingleTableHive T8, SingleTableHive T9
        |
        |LEFT JOIN SingleTableHive T10 ON T10.subject=T1.subject  AND  T10.predicate='http://localhost/vocabulary/bench/abstract'
        |LEFT JOIN SingleTableHive T6 ON T6.subject=T1.subject    AND  T6.predicate= 'http://swrc.ontoware.org/ontology#pages'
        |LEFT JOIN SingleTableHive T5 ON T5.subject=T1.subject    AND  T5.predicate='http://www.w3.org/2000/01/rdf-schema#seeAlso'
        |LEFT JOIN SingleTableHive T4 ON T4.subject=T1.subject    AND  T4.predicate='http://purl.org/dc/terms/partOf'
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
        |FROM SingleTableHive A1
        |LEFT JOIN SingleTableHive A2 ON A2.subject=A1.subject  AND  A2.predicate= 'http://swrc.ontoware.org/ontology#pages'
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
        |    SingleTableHive T1 , SingleTableHive T2 , SingleTableHive T3,
        |    SingleTableHive T4, SingleTableHive T5, SingleTableHive T6, SingleTableHive T7, SingleTableHive T8
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
        |    SingleTableHive T1 , SingleTableHive T2 , SingleTableHive T3,
        |    SingleTableHive T4, SingleTableHive T5, SingleTableHive T6
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





    /////////////////////////////////////query 11/////////////////////////////////////////////
    /* spark.sql(
       """
         |SELECT
         |T1.object AS ee
         |FROM MyRDFTable T1
         |WHERE T1.predicate='http://www.w3.org/2000/01/rdf-schema#seeAlso'
         |ORDER BY ee
         |LIMIT 10
         |
         | """.stripMargin).show */











  }

}
