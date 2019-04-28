package ee.ut.cs.bigdata.sp2bench.fileformatconversion

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SingleStatementTable {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkConversionSingleTable")
      .getOrCreate()

    val filePathCSV:String=args(0)
    val filePathAVRO:String=args(1)
    val filePathPARQUET:String=args(2)
    val filePathORC:String=args(3)

    val RDFDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePathCSV).toDF()

    RDFDF.write.format("com.databricks.spark.avro").save(filePathAVRO)
    RDFDF.write.parquet(filePathPARQUET)
    RDFDF.write.orc( filePathORC)

  }


}
