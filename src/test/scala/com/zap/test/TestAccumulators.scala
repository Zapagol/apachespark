//package com.zap.test
//
//import org.apache.spark.sql.SparkSession
//
///**
//  * Find maximum length word in a file.
//  *
//  *  (word) - line
//  *
//  *  Indicator - length = 4
//  *
//  *
//  *
//  */
//object TestAccumulators extends App {
//
//  val spark = SparkSession
//    .builder()
//    .getOrCreate()
//
//  val file = spark.sparkContext.textFile("file_path")
//  //val indicator = spark.sparkContext.longAccumulator("Word Lenth");
//  import spark.implicits._
//  val highestWordLength = file
//    .flatMap(line => line.split(" "))
//    .map( word => (word, word.length))
//    .toDF("word", "length")
//
//  highestWordLength.createOrReplaceTempView("wordTable")
//
//
//
//  `
//
//}
