package ee.ut.cs.bigdata.sp2bench.hive




  import org.apache.spark.sql.SparkSession

object VerticalTables {

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




    //val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/titles.csv").toDF()
    val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/title.csv").toDF()
    RDFDFTitle.createOrReplaceTempView("TitleRDF")
    spark.sql("create table if not exists Title as select * from TitleRDF")



    //val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/issued.csv").toDF()
    val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/issued.csv").toDF()
    RDFDFIssued.createOrReplaceTempView("IssuedDF")
    spark.sql("create table if not exists Issued as select * from IssuedDF")




    //val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/type.csv").toDF()
    val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/type.csv").toDF()
    RDFDFType.createOrReplaceTempView("TypeDF")
    spark.sql("create table if not exists Type as select * from TypeDF")




    //val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/creator.csv").toDF()
    val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/creator.csv").toDF()
    RDFDFCreator.createOrReplaceTempView("CreatorDF")
    spark.sql("create table if not exists Creator as select * from CreatorDF")



    //val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/bookTitle.csv").toDF()
    val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/booktitle.csv").toDF()
    RDFDFBookTitle.createOrReplaceTempView("BookTitleDF")
    spark.sql("create table if not exists BookTitle as select * from BookTitleDF")

    //val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/partOf.csv").toDF()
    val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/partOf.csv").toDF()
    RDFDFPartOf.createOrReplaceTempView("PartOfDF")
    spark.sql("create table if not exists PartOf as select * from PartOfDF")



    //val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/seeAlso.csv").toDF()
    val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/seeAlso.csv").toDF()
    RDFDFSeeAlso.createOrReplaceTempView("SeeAlsoDF")
    spark.sql("create table if not exists SeeAlso as select * from SeeAlsoDF")

    //val RDFDFPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/pages.csv").toDF()
    val RDFDFPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/pages.csv").toDF()
    RDFDFPages.createOrReplaceTempView("PagesDF")
    spark.sql("create table if not exists Pages as select * from PagesDF")


    //val RDFDFHomePage= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/homepages.csv").toDF()
    val RDFDFHomePage= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/homepage.csv").toDF()
    RDFDFHomePage.createOrReplaceTempView("HomePageDF")
    spark.sql("create table if not exists HomePage as select * from HomePageDF")


    //val RDFDFAbstract= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/abstract.csv").toDF()
    val RDFDFAbstract= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/abstract.csv").toDF()
    RDFDFAbstract.createOrReplaceTempView("AbstractDF")
    spark.sql("create table if not exists Abstract as select * from AbstractDF")


    //val RDFDFName= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/names.csv").toDF()
    val RDFDFName= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/name.csv").toDF()
    RDFDFName.createOrReplaceTempView("NameDF")
    spark.sql("create table if not exists Name as select * from NameDF")


    //val RDFDFJournal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/journal.csv").toDF()
    val RDFDFJournal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/journal.csv").toDF()
    RDFDFJournal.createOrReplaceTempView("inJournalDF")
    spark.sql("create table if not exists inJournal as select * from inJournalDF")



    val RDFDFSubClassOf= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/subClassOf.csv").toDF()
    RDFDFSubClassOf.createOrReplaceTempView("subclassOfDF")
    spark.sql("create table if not exists subclassof as select * from subclassOfDF")



    val RDFDFEditor= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/VerticalTables/editor.csv").toDF()
    RDFDFEditor.createOrReplaceTempView("editorDF")
    spark.sql("create table if not exists Editor as select * from editorDF")


    /*Specially Here for Query 9*/
    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/CSV/SingleTable/SingleTable.csv").toDF()
    RDFDF.createOrReplaceTempView("MyRDFTablePredicatesCombined")
    spark.sql("create table if not exists PredicatesCombined as select * from MyRDFTablePredicatesCombined");




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
        |    JOIN inJournal J1 ON A1.subject=J1.subject
        |    JOIN inJournal J2 ON J1.object=J2.object
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







    /////////////////////////////////////////////// q6/////////////////////////////////////////////////////////////////////////////////



    spark.time(spark.sql(
      """
        |SELECT
        |    L1.yr       AS yr,
        |    L1.name     AS name,
        |    L1.document AS document
        |FROM
        |    (
        |        SELECT
        |            RT1.subject AS class,
        |            RT2.subject      AS document,
        |            DI.object      AS yr,
        |            DC.object   AS author,
        |            FN.object      AS name
        |        FROM
        |            subclassof   RT1
        |            JOIN Type RT2      ON RT1.subject=RT2.object
        |            JOIN Issued DI ON DI.subject=RT2.subject
        |            JOIN Creator DC     ON DC.subject=DI.subject
        |            JOIN Name FN      ON DC.object=FN.subject
        |        WHERE RT1.object='http://xmlns.com/foaf/0.1/Document'
        |    ) AS L1
        |
        |    LEFT JOIN
        |    (
        |        SELECT
        |			RT1.subject AS class,
        |            RT2.subject      AS document,
        |            DI.object      AS yr,
        |            DC.object   AS author
        |
        |        FROM
        |            subclassof RT1
        |            JOIN Type RT2      ON RT1.subject=RT2.object
        |            JOIN Issued DI ON DI.subject=RT2.subject
        |            JOIN Creator DC     ON DC.subject=DI.subject
        |
        |        WHERE RT1.object='http://xmlns.com/foaf/0.1/Document'
        |
        |    ) AS L2
        |    ON L1.author=L2.author AND L2.yr<L1.yr
        |WHERE L2.author IS NULL
      """.stripMargin

    ).show())




    /////////////////////////////////////////////// Query 8 Vertical Tables  /////////////////////////////////////////////////////////////////////////////////



    spark.time(spark.sql(
      """
        | SELECT DISTINCT
        |    name
        |FROM
        |    Type RT
        |    JOIN Name FN  ON RT.subject=FN.subject
        |    JOIN
        |    (
        |        SELECT
        |            name,
        |            erdoes
        |        FROM
        |        (
        |            SELECT
        |                FN2.object AS name,
        |                DC1.object AS erdoes
        |            FROM
        |                Creator DC1
        |                JOIN Creator DC2 ON DC1.subject=DC2.subject
        |                JOIN Name FN2  ON DC2.object=FN2.subject
        |            WHERE
        |                NOT DC1.object=DC2.object
        |        ) AS L
        |        UNION
        |        (
        |            SELECT
        |                FN2.object AS name,
        |                DC1.object AS erdoes
        |            FROM
        |                Creator DC1
        |                JOIN Creator DC2 ON DC1.subject=DC2.subject
        |                JOIN Creator DC3 ON DC2.object=DC3.object
        |                JOIN Creator DC4 ON DC3.subject=DC4.subject
        |                JOIN Name FN2  ON DC4.object=FN2.subject
        |            WHERE
        |                NOT DC2.object=DC1.object
        |                AND NOT DC3.subject=DC1.subject
        |                AND NOT DC4.object=DC1.object
        |                AND NOT DC2.object=DC4.object
        |        )
        |    ) AS R ON FN.subject=R.erdoes
        |WHERE
        |    RT.object='http://xmlns.com/foaf/0.1/Person'
        |    AND FN.object='Paul Erdoes'
      """.stripMargin).show())


    /////////////////////////////////// Query 9 vertical Tables/////////////////


    spark.time(spark.sql(
      """
        |SELECT DISTINCT L.predicate
        |FROM
        |    (
        |
        |        SELECT
        |            RT.subject,
        |            T.predicate
        |        FROM
        |            Type RT
        |            JOIN PredicatesCombined AS T ON RT.subject=T.object
        |        WHERE
        |            RT.object='http://xmlns.com/foaf/0.1/Person'
        |
        |
        |        UNION
        |
        |
        |        SELECT
        |            RT.subject,
        |            T.predicate
        |        FROM
        |            Type RT
        |            JOIN  PredicatesCombined  AS T ON RT.subject=T.subject
        |        WHERE
        |            RT.object='http://xmlns.com/foaf/0.1/Person'
        |
        |    ) AS L
        |""".stripMargin).show())



    /////////////////////////////// Query 10 ///////////////////////////////////////


    spark.time(spark.sql(
      """
        |
        |SELECT
        |    L.subject AS subject, L.predicate AS predicate
        |FROM
        |(
        |SELECT A.subject, "dc:#Creator" As predicate  FROM Creator A WHERE  A.object='http://localhost/persons/Paul_Erdoes'
        |UNION
        |SELECT E.subject , "dc:#Editor" As predicate  FROM Editor E  WHERE  E.object='http://localhost/persons/Paul_Erdoes'
        |) AS L
      """.stripMargin).show())



    ///////////////////////////////////// Query 11 //////////////////////////////////////////

    spark.time(spark.sql(
      """
        |SELECT
        |    RSA.object AS ee
        |FROM
        |    SeeAlso RSA
        |
        |ORDER BY ee DESC
        |LIMIT 10
        |
        | """.stripMargin).show)








  }

}
