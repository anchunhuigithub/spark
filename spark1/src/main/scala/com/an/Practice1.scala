package com.an

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * scala spark study
  */
object Practice1 {

  // 创建得到logger 对象
  val logger = LoggerFactory.getLogger(Practice1.getClass)

  // main方法
  def main(args: Array[String]): Unit = {

    //创建sparkConf 并设置 appname
    val sparkConf = new SparkConf().setAppName("practice1").setMaster("local[*]")
    //创建sparkContext
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.textFile(args(0)).
      flatMap(_.split(" ")).
      map((_, 1)).
      reduceByKey(_ + _, 1).
      sortBy(_._2, false).
      saveAsTextFile(args(1))

    logger.info("complete")

    sparkContext.stop()
  }


}
