package ee.ut.cs.bigdata.sp2bench.avro


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





    //val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/titles.csv").toDF()
    val RDFDFTitle = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Title").toDF()
    //RDFDFTitle.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/title")
    RDFDFTitle.createOrReplaceTempView("Title")
   // spark.sql("create table if not exists Title as select * from TitleRDF")



    //val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/issued.csv").toDF()
    val RDFDFIssued = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Issued").toDF()
    //RDFDFIssued.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/issued")
    RDFDFIssued.createOrReplaceTempView("Issued")
    //spark.sql("create table if not exists Issued as select * from IssuedDF")




    //val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/type.csv").toDF()
    val RDFDFType = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Type").toDF()
    //RDFDFType.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/type")
    RDFDFType.createOrReplaceTempView("Type")
    //spark.sql("create table if not exists Type as select * from TypeDF")




    //val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/creator.csv").toDF()
    val RDFDFCreator = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Creator").toDF()
    //RDFDFCreator.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/creator")
    RDFDFCreator.createOrReplaceTempView("Creator")
    //spark.sql("create table if not exists Creator as select * from CreatorDF")



    //val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/bookTitle.csv").toDF()
    val RDFDFBookTitle = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/BookTitle").toDF()
    //RDFDFBookTitle.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/booktitle")
    RDFDFBookTitle.createOrReplaceTempView("BookTitle")
    //spark.sql("create table if not exists BookTitle as select * from BookTitleDF")

    //val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/partOf.csv").toDF()
    val RDFDFPartOf = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/PartOf").toDF()
    //RDFDFPartOf.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/partOf")
    RDFDFPartOf.createOrReplaceTempView("PartOf")
    //spark.sql("create table if not exists PartOf as select * from PartOfDF")



    //val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/seeAlso.csv").toDF()
    val RDFDFSeeAlso = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/SeeAlso").toDF()
    //RDFDFSeeAlso.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/seeAlso")
    RDFDFSeeAlso.createOrReplaceTempView("SeeAlso")
    //spark.sql("create table if not exists SeeAlso as select * from SeeAlsoDF")

    //val RDFDFPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/pages.csv").toDF()
    val RDFDFPages= spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Pages").toDF()
    //RDFDFPages.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/pages")
    RDFDFPages.createOrReplaceTempView("Pages")
    //spark.sql("create table if not exists Pages as select * from PagesDF")


    //val RDFDFHomePage= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/homepages.csv").toDF()
    val RDFDFHomePage= spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/HomePage").toDF()
    //RDFDFHomePage.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/homepage")
    RDFDFHomePage.createOrReplaceTempView("HomePage")
    //spark.sql("create table if not exists HomePage as select * from HomePageDF")


    //val RDFDFAbstract= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/abstract.csv").toDF()
    val RDFDFAbstract= spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Abstract").toDF()
    //RDFDFAbstract.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/abstract")
    RDFDFAbstract.createOrReplaceTempView("Abstract")
    //spark.sql("create table if not exists Abstract as select * from AbstractDF")


    //val RDFDFName= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/names.csv").toDF()
    val RDFDFName= spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Name").toDF()
    //RDFDFName.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/name")
    RDFDFName.createOrReplaceTempView("Name")
    //spark.sql("create table if not exists Name as select * from NameDF")


    //val RDFDFJournal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///data2/VerticalPartionnedTables/VerticalPartitionedTables100/journal.csv").toDF()
    val RDFDFJournal= spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/VerticalTables/Journal").toDF()
    //RDFDFJournal.write.format("com.databricks.spark.avro").save("./ORC/VerticalTables/journal")
    RDFDFJournal.createOrReplaceTempView("inJournal")
    //spark.sql("create table if not exists inJournal as select * from inJournalDF")


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








  }

}
