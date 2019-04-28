package ee.ut.cs.bigdata.sp2bench.fileformatconversion

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object RDFBenchProertyTables {

  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime

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


    val RDFDFJournal = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/Journal.csv").toDF()

    RDFDFJournal.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/Journal")
    RDFDFJournal.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFJournal.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")

    val RDFDFinProceedingArticle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/inProceedingArticle.csv").toDF()

    RDFDFinProceedingArticle.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/InProceedingArticle")
    RDFDFinProceedingArticle.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFinProceedingArticle.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")

    val RDFDFPerson = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/person.csv").toDF()

    RDFDFPerson.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/Person")
    RDFDFPerson.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFPerson.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")


    val RDFDFJournalArticle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/JournalArticle.csv").toDF()

    RDFDFJournalArticle.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/JournalArticle")
    RDFDFJournalArticle.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFJournalArticle.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")



    val RDFDFDocument = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/Document.csv").toDF()

    RDFDFDocument.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/Document")
    RDFDFDocument.write.parquet("file:///C:/ExprimentRDFTest/Parquet/Document")
    RDFDFDocument.write.orc("file:///C:/ExprimentRDFTest/ORC/Document")




  }


}
