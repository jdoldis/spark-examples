/**
 * Spark Word Count Application
 */

package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountRDD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val inputFile = sc.textFile(args(0))
    val wordCounts = inputFile
      .flatMap(line => line.split(" ")) // split the collection of lines into a collection of words
      .map(word => (word, 1))           // turn the collection of words into a collection of (word,1)
      .reduceByKey(_+_)                 // count the occurrences of each word
    wordCounts.collect.foreach(println)
    sc.stop
  }
}
