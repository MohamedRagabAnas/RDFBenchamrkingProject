package ee.ut.cs.bigdata.sp2bench.avro


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ProertyTables {

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




    val RDFDFJournal = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/PropertyTables/Journal").toDF()
    //RDFDFJournal.write.format("com.databricks.spark.avro").save("./AVRO/PropertyTables/Journal")
    RDFDFJournal.createOrReplaceTempView("Journal")
    //spark.sql("create table if not exists Journal as select * from JournalDF")



    val RDFDFinProceedingArticle = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/PropertyTables/InProceedingArticle").toDF()
    //RDFDFinProceedingArticle.write.format("com.databricks.spark.avro").save("./AVRO/PropertyTables/InProceedingArticle")
    RDFDFinProceedingArticle.createOrReplaceTempView("inProceedingArticle")
    //spark.sql("create table if not exists inProceedingArticle as select * from inProceedingArticleDF")


    val RDFDFPerson = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/PropertyTables/Person").toDF()
    //RDFDFPerson.write.format("com.databricks.spark.avro").save("./AVRO/PropertyTables/Person")
    RDFDFPerson.createOrReplaceTempView("Person")
    //spark.sql("create table if not exists Person as select * from PersonDF")

    val RDFDFJournalArticle = spark.read.format("com.databricks.spark.avro").load("hdfs://quickstart:8020/user/cloudera/RDFBenchHDFS/AVRO/PropertyTables/JournalArticle").toDF()
    //RDFDFJournalArticle.write.format("com.databricks.spark.avro").save("./AVRO/PropertyTables/JournalArticle")
    RDFDFJournalArticle.createOrReplaceTempView("JournalArticle")
    //spark.sql("create table if not exists JournalArticle as select * from JournalArticleDF")


    /////////////////////\\\SP2Bench Query1 Prop. Tables\\\/////////////////////

    //val tq1 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT
        |J.issued AS yr
        |FROM
        |Journal J
        |WHERE
        |J.title='Journal 1 (1940)'
        | """.stripMargin).show)

    //val durationQuery1 = (System.nanoTime - tq1) / 1e9d
    //println(durationQuery1)

    /////////////////////\\\SP2Bench Query2  Prop. Tables\\\/////////////////////


    //val tq2 = System.nanoTime

    spark.time(spark.sql(
      """
        |SELECT
        |    D.subject         AS inproc,
        |    D.creator       	 AS author,
        |    D.booktitle  	   AS booktitle,
        |    D.title      	   AS title,
        |    D.partOf          AS proc,
        |    D.seeAlso         AS ee,
        |    D.pages      	   AS pages,
        |    D.homepage   	   AS url,
        |    D.issued     	   AS yr,
        |    D.abstract   	   AS abstract
        |
        |FROM inProceedingArticle D
        |ORDER BY yr
        | """.stripMargin).show())

    //val durationQuery2 = (System.nanoTime - tq2) / 1e9d
    //println(durationQuery2)

    /////////////////////\\\SP2Bench Query3  Prop. Tables\\\/////////////////////
    //val tq3 = System.nanoTime

    spark.time(spark.sql(
      """
        SELECT DISTINCT A.subject FROM JournalArticle A
        |WHERE
        |A.pages IS NOT NULL
        | """.stripMargin).show)

    //val durationQuery3 = (System.nanoTime - tq3) / 1e9d
    //println(durationQuery3)



    ////////////////////////////////////////// SP2Bench Query 4 Prop. Tables/////////////////////////

    //val tq4 = System.nanoTime

    spark.time(spark.sql(
      """
        SELECT DISTINCT
        |Pe1.name AS name1 , Pe2.name AS name1
        |FROM JournalArticle A1 , JournalArticle A2 , Person Pe1 , Person Pe2
        |WHERE
        |A1.creator=Pe1.subject  AND
        |A2.creator=Pe2.subject  AND
        |A1.journal=A2.journal   AND
        |Pe1.name<Pe2.name
        | """.stripMargin).show())

    // val durationQuery4 = (System.nanoTime - tq4) / 1e9d
    // println(durationQuery4)

    ///////////////////////////////Sp2Bench Query 5 Prop. Tables/////////////////////////////////////////

    spark.time(spark.sql(
      """
        |SELECT DISTINCT
        |    Pe1.subject AS person,
        |    Pe1.name    AS name
        |FROM
        |    JournalArticle P1, Person pe1, Person Pe2, inProceedingArticle P2
        |WHERE
        |    P1.creator=Pe1.subject  AND
        |    P2.creator=Pe2.subject  AND
        |    Pe1.name=Pe2.name
        |    """.stripMargin).show())







  }

}
