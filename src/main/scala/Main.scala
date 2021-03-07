package example
import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import org.apache.spark

object Main {
  val INPUT_FILE = "data/test.csv"

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("asgn").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    val lineItems = sc.textFile(INPUT_FILE).flatMap(_.split("\n")).map(_.split(","));

    val cleaned = lineItems.map(x => x.slice(5,8) ++ x.slice(9, 15) :+ x(18))
    cleaned.take(3).foreach(x => {
      x.foreach(println(_))
    })

  }

  val manufMap = immutable.Map("ford" -> 0,
                              "chevrolet" -> 1,
                              "toyota" -> 2,
                              "jeep" -> 3,
                              "nissan" -> 4,
                              "honda" -> 5,
                              "ram" -> 6,
                              "gmc" -> 7,
                              "dodge" -> 8,
                              "hyundai" -> 9)

  val condMap = immutable.Map("new" -> 0,
                                "like new" -> 1,
                                "excellent" -> 2,
                                "good" -> 3,
                                "fair" -> 4,
                                "salvage" -> 5)

  val cylindMap = immutable.Map("4 cylinders" -> 0,
                                "6 cylinders" -> 1,
                                "8 cylinders" -> 2)


  // price, year, manuf, condition, cylinders, fuel, odom, title, trans, type
  def clean(entry: Array[String]) : Array[String] = {

  }
}
