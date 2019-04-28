package ee.ut.cs.bigdata.sp2bench.fileformatconversion

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RDFBenchVerticalPartionedTables {

  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val RDFDFTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/title.csv").toDF()
    RDFDFTitle.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Title")
    RDFDFTitle.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFTitle.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")


    val RDFDFIssued = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/issued.csv").toDF()

    RDFDFIssued.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Issued")
    RDFDFIssued.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFIssued.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")




    val RDFDFType = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/type.csv").toDF()

    RDFDFType.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Type")
    RDFDFType.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFType.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")





    val RDFDFCreator = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/creator.csv").toDF()

    RDFDFCreator.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Creator")
    RDFDFCreator.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFCreator.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")





    val RDFDFBookTitle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/booktitle.csv").toDF()

    RDFDFBookTitle.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/BookTitle")
    RDFDFBookTitle.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFBookTitle.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")




    val RDFDFPartOf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/partOf.csv").toDF()

    RDFDFPartOf.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/partOf")
    RDFDFPartOf.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFPartOf.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")





    val RDFDFSeeAlso = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/seeAlso.csv").toDF()

    RDFDFSeeAlso.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/SeeAlso")
    RDFDFSeeAlso.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFSeeAlso.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")





    val RDFDFPages= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/pages.csv").toDF()

    RDFDFPages.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Pages")
    RDFDFPages.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFPages.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")




    val RDFDFHomePage= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/homepage.csv").toDF()

    RDFDFHomePage.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/HomePage")
    RDFDFHomePage.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFHomePage.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")



    val RDFDFAbstract= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/abstract.csv").toDF()

    RDFDFAbstract.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Abstract")
    RDFDFAbstract.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFAbstract.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")




    val RDFDFName= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/name.csv").toDF()

    RDFDFName.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Name")
    RDFDFName.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFName.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")




    val RDFDFJournal= spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/VerticalTables/journal.csv").toDF()

    RDFDFJournal.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/VerticalTables/Journal")
    RDFDFJournal.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFJournal.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")




  }



}
