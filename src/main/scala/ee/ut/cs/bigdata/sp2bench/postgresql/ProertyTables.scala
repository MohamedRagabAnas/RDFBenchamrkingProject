package ee.ut.cs.bigdata.sp2bench.postgresql

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




    val RDFDFJournal = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "journal")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()


    RDFDFJournal.createOrReplaceTempView("Journal")




    val RDFDFinProceedingArticle = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "inproceedingarticle")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()

    RDFDFinProceedingArticle.createOrReplaceTempView("inProceedingArticle")



    val RDFDFPerson = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "person")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()


    RDFDFPerson.createOrReplaceTempView("Person")


    val RDFDFJournalArticle = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "journalarticle")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()

    RDFDFJournalArticle.createOrReplaceTempView("JournalArticle")


    val RDFDFDocument = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5432/rdfbenchmarking")
      .option("dbtable", "document")
      .option("user", "ragab")
      .option("password", "engmohamed")
      .load()

    RDFDFDocument.createOrReplaceTempView("Document")




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




    //////////////////////////////////// Query 6  Property Tables /////////////////////


    spark.time(println(spark.sql(
      """
        |SELECT
        |    D1.issued   AS yr,
        |    Pe1.name    AS name,
        |    D1.document AS document
        |FROM
        |    Document D1
        |    JOIN Person Pe1  ON Pe1.subject=D1.creator
        |WHERE
        |    NOT EXISTS (
        |        SELECT *
        |        FROM
        |            Document D2
        |            JOIN Person Pe2  ON Pe2.subject=D2.creator
        |        WHERE
        |            pe1.subject=pe2.subject
        |            AND D2.issued<D1.issued
        |    ) AND D1.issued IS NOT NULL
        |    """.stripMargin).show()))




    /////////////////////////////////////////// Query 8 Property Tables ///////////////////////////


    spark.time(spark.sql(
      """
     |SELECT DISTINCT name
     |
     |FROM (
     |(
     |   SELECT
     |        Pe2.name AS name
     |    FROM
     |        Person Pe1,Person Pe2,
     |		    Document D1  JOIN Document D2 ON D1.document=D2.document
     |    WHERE
     |        D1.creator=Pe1.subject AND D2.creator=Pe2.subject
     |        AND Pe1.name='Paul Erdoes'
     |        AND Pe2.name <>'Paul Erdoes'
     |)
     |
     |
     |UNION
     |(
     |SELECT
     |        Pe3.name AS name
     |    FROM
     |        Person Pe1, Person Pe2, Person Pe3,
     |		    Document D1 JOIN  Document D2 ON D1.document=D2.document,
     |        Document D3 JOIN  Document D4 ON D3.document=D4.document
     |    WHERE
     |        D1.document<>D3.document
     |        AND D1.creator=Pe1.subject
     |        AND D2.creator=Pe2.subject
     |        AND D3.creator=Pe2.subject
     |        AND D4.creator=Pe3.subject
     |        AND Pe1.name = 'Paul Erdoes'
     |        AND Pe2.name <>'Paul Erdoes'
     |        AND Pe3.name <>'Paul Erdoes'
     |        AND D1.document <> D3.document
     |
     |)) AS dist

          """.stripMargin).show())




    /*  // //////////////////////////////////////////  Testing Stuff in Query 8 each part of the query separately /////////////////////////////////////


        println(spark.sql(
          """
            |    SELECT DISTINCT
            |        Pe2.name AS name
            |    FROM
            |        Person Pe1,Person Pe2,
            |		    Document D1  JOIN Document D2 ON D1.document=D2.document
            |    WHERE
            |        D1.creator=Pe1.subject AND D2.creator=Pe2.subject
            |        AND Pe1.name='Paul Erdoes'
            |        AND Pe2.name <>'Paul Erdoes'
          """.stripMargin).count())



        println(spark.sql(
          """
            |SELECT DISTINCT
            |        Pe3.name AS name
            |    FROM
            |        Person Pe1, Person Pe2, Person Pe3,
            |		     Document D1  JOIN Document D2 ON  D1.document=D2.document,
            |        Document D3  JOIN Document D4 ON D3.document=D4.document
            |    WHERE
            |
            |        D1.creator=Pe1.subject
            |        AND D2.creator=Pe2.subject
            |
            |        AND D3.creator=Pe2.subject
            |        AND D4.creator=Pe3.subject
            |
            |        AND Pe1.name  = 'Paul Erdoes'
            |        AND Pe2.name <>'Paul Erdoes'
            |        AND Pe3.name <> 'Paul Erdoes'
            |
            |        AND D1.document <> D3.document
            |
            |
          """.stripMargin).count())


    */


    ///////////////////////////////////////////////// Query 9 Property Tables  {Not/Applicable} //////////////////////////

    /* N/A*/


    ///////////////////////////////////////////// Query 10 Property Tables //////////////////////////

    spark.time(spark.sql(
      """
        |SELECT
        |     D.document  AS subject,
        |    'dc:creator' AS predicate
        |FROM
        |    Person A
        |    JOIN Document D    ON D.creator=A.subject
        |WHERE
        |    A.name='Paul Erdoes'
        |
        |UNION
        |
        |SELECT
        |     D.document    AS subject,
        |    'swrc:editor' AS predicate
        |FROM
        |    Person E
        |    JOIN Document D ON D.editor=E.subject
        |WHERE
        |    E.name='Paul Erdoes'
      """.stripMargin).show())


    ///////////////////////////////////////// Query 11 ////////////////////////////////////////


    spark.time(spark.sql(
      """
        |SELECT
        |    P.seeAlso as ee
        |FROM
        |     Document P
        |
        |ORDER BY ee DESC
        |LIMIT 10
      """.stripMargin).show())




  }

}
