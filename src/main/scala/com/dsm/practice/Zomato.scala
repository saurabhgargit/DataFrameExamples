package com.dsm.practice

import org.apache.spark.sql.SparkSession

object Zomato {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").appName("Dataframe Example").getOrCreate()
    sparkSession.sparkContext.setLogLevel("INFO")
    //sparkSession.sparkContext.setLogLevel("")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIA2477WNKNZ6ZYPAOZ")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val zomatoRDD = sparkSession.sparkContext.textFile("s3n://new-saurabh/zomato.csv")


    //zomatoRDD.foreach(println)
    val data = zomatoRDD.count()
    println(data)
  }

}
