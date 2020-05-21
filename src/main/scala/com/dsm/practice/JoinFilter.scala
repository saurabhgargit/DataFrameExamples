package com.dsm.practice

import com.dsm.model.{Course, Demographic, Finance}
import org.apache.spark.sql.SparkSession

object JoinFilter {

  def main(args: Array[String]): Unit = {
    var sparkSession = SparkSession.builder().master("local[*]").appName("Join Exp").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIA2477WNKNZ6ZYPAOZ")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")

    val demographicsRDD = sparkSession.sparkContext.textFile("s3n://new-saurabh/demographic.csv")
    val financesRDD = sparkSession.sparkContext.textFile("s3n://new-saurabh/finances.csv")
    val coursesRDD = sparkSession.sparkContext.textFile("s3n://new-saurabh/course.csv")

    val demographicPairRdd = demographicsRDD.map(record =>record.split(","))
      .map(record =>
        Demographic(record(0).toInt,
          record(1).toInt,
          record(2).toBoolean,
          record(3),
          record(4),
          record(5).toBoolean,
          record(6).toBoolean,
          record(7).toInt
        )
      ).map(demographic => (demographic.id, demographic))

    val financePairRdd = financesRDD.map(record =>record.split(","))
      .map(record =>
        Finance(record(0).toInt,
        record(1).toBoolean,
        record(2).toBoolean,
        record(3).toBoolean,
        record(4).toInt
        )
      ).map(finance => (finance.id,finance))

    val courses = coursesRDD.map(record => record.split(","))
      .map(record => Course(record(0).toInt, record(1)))
      .map(course => (course.id, course))

//    demographicPairRdd.join(financePairRdd)
//      .filter(p => p._2._1.country=="Switzerland"
//      && p._2._2.hasDebt
//      && p._2._2.hasFinancialDependents)
//      .map(p => (p._2._1.courseId, (p._2._1,p._2._2))).join(courses)
//      .map(p =>(p._2._1._1.id, ))
//      .foreach(println)
  }

}

