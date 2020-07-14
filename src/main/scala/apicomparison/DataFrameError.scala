/**
 * Example to show the difference between Dataframes and Datasets
 */

package apicomparison

import org.apache.spark.sql.SparkSession

object DataFrameError {
  case class Price(date:String,price:Int)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("DataFrameError").getOrCreate()
    import spark.implicits._

    val pricedf = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("data/prices.csv")

    pricedf.filter("price > 2").show
    // The below line will compile and throw an error at runtime
    pricedf.filter("columnthatdoesntexist > 10").show

    val priceds = pricedf.as[Price]

    // With type safety the runtime error on line 24 is impossible - if you 
    // reference a column that doesn't exist the compiler will pick it up
    priceds.filter(_.price > 2).show

    spark.stop
  }
}
