package ee.ut.cs.bigdata.sp2bench.mysql


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





    val jdbcDF = spark.read.format("jdbc").options(
      Map("url" ->  "jdbc:mysql://localhost:3306/rdfbenchmarking?user=root&password=cloudera",
        "dbtable" -> "rdfbenchmarking.singlestatementtable",
        "fetchSize" -> "10000",
        "partitionColumn" -> "subject", "lowerBound" -> "1", "upperBound" -> "20", "numPartitions" -> "10"
      )).load()
    jdbcDF.createOrReplaceTempView("SingleTable")

    jdbcDF.show(30)



    ////////////////////////////////////////// To be Continued !!!!!!   mysql load from CSV  command on DSKTop need to be reviweed alot of empty  lines here //////////////////////////////////////



  }

}
