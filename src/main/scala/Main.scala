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

    val cleaned = lineItems.map(x => x.slice(5,8) ++ x.slice(9, 15)).map(entry => clean(entry)).filter(_.length == 9)

    println(cleaned.count())
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

  val fuelMap = immutable.Map("gas" -> 0,
                              "diesel" -> 1)

  val titleMap = immutable.Map("clean" -> 0,
                              "rebuilt" -> 1,
                              "salvage" -> 2)

  val transMap = immutable.Map("automatic" -> 0,
                                "manual" -> 1)

  // price, year, manuf, condition, cylinders, fuel, odom, title, trans
  def clean(entry: Array[String]) : List[Double] =  {
    var retArray = mutable.MutableList[Double]()
    for (i <- 0 to 8) {
      if (i == 0 || i == 1 || i == 6) {
        val a = try {
          entry(i).toDouble
        } catch {
          case e : Exception => ""
        }
        if (a == "")
          return List(0.0)
        
        retArray = retArray :+ a.toString.toDouble
      }
      else
        retArray = retArray :+ 0.0
    }
    if (manufMap.contains(entry(2)))
      retArray(2) = manufMap(entry(2))
    else
      return List(0.0)

    if (condMap.contains(entry(3)))
      retArray(3) = condMap(entry(3))
    else
      return List(0.0)

    if (cylindMap.contains(entry(4)))
      retArray(4) = cylindMap(entry(4))
    else
      return List(0.0)

    if (fuelMap.contains(entry(5)))
      retArray(5) = fuelMap(entry(5))
    else
      return List(0.0)

    if (titleMap.contains(entry(7)))
      retArray(7) = titleMap(entry(7))
    else
      return List(0.0)

    if (transMap.contains(entry(8)))
      retArray(8) = transMap(entry(8))
    else
      return List(0.0)

    return retArray.toList

  }
}
