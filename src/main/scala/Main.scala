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
  val TRAIN = 0.90
  val TEST = 0.10
  val N = 10

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("asgn").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    val lineItems = sc.textFile(INPUT_FILE).flatMap(_.split("\n")).map(_.split(","));

    val cleaned = lineItems.map(x => x.slice(5,7) ++ x.slice(9, 15)).map(entry => clean(entry)).filter(_.length == 8).persist()

    val training_rdd = cleaned.sample(withReplacement = false, TRAIN)
    val test = cleaned.subtract(training_rdd).collect()

    val correctPrice = test.map(_.head)
    val predictedPrice = test.map(e => kNN(e.tail, training_rdd))

    correctPrice.zip(predictedPrice).foreach(println)
  }

  def kNN(toPredict : List[Double], vals : RDD[List[Double]]) : Double = {
    vals.map(entry => (calcDistance(toPredict, entry.tail), entry.head))
      .sortByKey(ascending = true)
      .take(N).map({ case (_, price) => price }).sum / N
  }

  def calcDistance(p1 : List[Double], p2 : List[Double]) : Double = {
    val squared = p1.zip(p2).map(z => math.pow(z._1 - z._2, 2)).sum
    math.sqrt(squared)
  }

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

  // price, year, condition, cylinders, fuel, odom, title, trans
  def clean(entry: Array[String]) : List[Double] =  {
    var retArray = mutable.MutableList[Double]()
    for (i <- 0 to 7) {
      if (i == 0 || i == 1 || i == 5) {
        val a = try {
          entry(i).toDouble
        } catch {
          case e : Exception => ""
        }
        if (a == "")
          return List(0.0)
        if(i == 1 || i == 5) {
          retArray = retArray :+ 0.0
        }
        else {
          retArray = retArray :+ a.toString.toDouble
        }
      }
      else
        retArray = retArray :+ 0.0
    }
    if (condMap.contains(entry(2)))
      retArray(2) = condMap(entry(2))
    else
      return List(0.0)

    if (cylindMap.contains(entry(3)))
      retArray(3) = cylindMap(entry(3))
    else
      return List(0.0)

    if (fuelMap.contains(entry(4)))
      retArray(4) = fuelMap(entry(4))
    else
      return List(0.0)

    if (titleMap.contains(entry(6)))
      retArray(6) = titleMap(entry(6))
    else
      return List(0.0)

    if (transMap.contains(entry(7)))
      retArray(7) = transMap(entry(7))
    else
      return List(0.0)

    return retArray.toList

  }
}
