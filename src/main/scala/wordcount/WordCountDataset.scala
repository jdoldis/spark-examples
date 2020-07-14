/**
 * Spark Word Count Application using the Dataset API
 */

package wordcount

import org.apache.spark.sql.SparkSession

object WordCountDataset {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("WordCount").getOrCreate()
    import spark.implicits._

    val inputFile = spark.read.textFile(args(0)) // A text file is a Dataset[String]
    val wordCounts = inputFile
      .flatMap(line => line.split(" "))
      .groupByKey(identity)
      .count
    wordCounts.show
    spark.stop
  }
}
