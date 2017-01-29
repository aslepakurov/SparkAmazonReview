package com.aslepakurov.spark

import java.text.BreakIterator
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.reflect.io.File

/**
  * Author: Slepakurov Andrew
  *
  * Created on 01/29/17.
  */
object SparkLocalRunner {
  val API_CONCURRENCY_LIMIT = 100
  val API_CHARACTER_LIMIT = 1000
  val THREAD_POOL: ExecutorService = Executors.newFixedThreadPool(API_CONCURRENCY_LIMIT)

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("File input and directory output should be present.")
      System.exit(1)
    }

    val input = args(0)
    val outputDir = args(1)

    val sessionContext = SparkSession.builder()
      .master("local[4]")
      .appName("AmazonReview")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()

    val sqlContext = sessionContext.sqlContext
    val sparkContext = sessionContext.sparkContext

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    try {

      val reviews = sessionContext.read.option("header","true").csv(input).select($"ProductId", $"ProfileName", $"Text").cache()

//    1) Finding 1000 most active users (profile names)
      val reviewsByProfileName = reviews
        .groupBy($"ProfileName")
        .count()
        .sort(desc("count"))
        .map(row => row.getAs[String](0))
        .limit(1000)
//    2) Finding 1000 most commented food items (item ids).
      val reviewsByProductId = reviews
        .groupBy($"ProductId")
        .count()
        .sort(desc("count"))
        .map(row => row.getAs[String](0))
        .limit(1000)
//    3) Finding 1000 most used words in the reviews
      val words = reviews
        .map(row => row.getAs[String](2).split(" "))
        .flatMap(words => words)
        .map(word => word.replaceAll("[^A-Za-z]+", ""))
        .filter(word => word.matches("[a-zA-Z]+"))
        .groupBy($"value")
        .count()
        .sort(desc("count"))
        .map(row => row.getAs[String](0))
        .limit(1000)

//    Output first 20 to console
      reviewsByProductId.show()
      reviewsByProductId.show()
      words.show()

//    Output to file
      reviewsByProfileName.write.format("text").save(outputDir+File.separator+"profileNames")
      reviewsByProductId.write.format("text").save(outputDir+File.separator+"productIds")
      words.write.format("text").save(outputDir+File.separator+"words.out")

//    4) Translate all the reviews using Google Translate API(mock).
      reviews
        .map(row => row.getAs[String](2))
        .foreach(msg => {
          translateMessage(msg)
        })
    }
    catch {
      case e:Throwable =>
        e.printStackTrace()
        System.exit(0)
    }
    finally {
      sparkContext.stop()
      System.exit(0)
    }
  }

  private def translateMessage(msg: String) = {
    if (msg.length < API_CHARACTER_LIMIT) {
      val translation = THREAD_POOL.submit(getTranslateHandler(msg))
      translation.get().asInstanceOf[String]
    } else {
      val sentences = separateSentences(msg)
      if (sentences.length == 1) {
        println("No translation can be provided.")
      }
      for (sentence <- sentences) {
        val translation = THREAD_POOL.submit(getTranslateHandler(sentence))
        translation.get().asInstanceOf[String]
      }
    }
  }

  private def getTranslateHandler(msg: String) = {
    new MockHandler(msg)
  }

  private def separateSentences(msg: String) = {
    val sentences = ArrayBuffer[String]()
    val iterator = BreakIterator.getSentenceInstance()
    iterator.setText(msg)
    var start = iterator.first()
    var end = iterator.next()
    while (end != BreakIterator.DONE) {
      val substring = msg.substring(start, end)
      sentences += substring
      start = end
      end = iterator.next()
    }
    sentences
  }

  class MockHandler(msg: String) extends Runnable {
    def run() = {
      val sleep = ThreadLocalRandom.current().nextInt(0, 200)
      println(Thread.currentThread.getName + " = " + sleep + " ms = " + msg + " | ")
    }
  }
}
