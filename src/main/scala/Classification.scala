import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{mutable, _}
import scala.collection.mutable.HashMap

object Classification {
  val INPUT_FILE = "data/test.csv"
  val TRAINING_NUM = 100.0 // Number of entries to train on, or change equation in line 35 to a decimal for a percentage to train on
  val K = 3
  val numberOfWindows = 3

  var yearMin = 3000.0
  var yearMax = 0.0
  var odomMin = 100000.0
  var odomMax = 0.0
  var priceMin = Double.MaxValue
  var priceMax = 0.0

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("asgn").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    val lineItems = sc.textFile(INPUT_FILE).flatMap(_.split("\n")).map(_.split(","));

    val cleaned = lineItems.map(x => x.slice(5,7) ++ x.slice(9, 15)).map(entry => clean(entry)).filter(x => x.length == 8 && x(0) != 0).persist()

    val normalized = cleaned.map(entry => normalize(entry))

    val training_rdd = normalized.sample(withReplacement = false, 1 - (TRAINING_NUM / normalized.count())).persist()
    val test = normalized.subtract(training_rdd).collect()

    val correctPrice = test.map(_.head.toInt)
    val predictedPrice = test.map(e => kNN(e.tail, training_rdd)).toList
    metrics(sc, correctPrice.toList, predictedPrice)
    //val comparisons = correctPrice.zip(predictedPrice)
    //comparisons.foreach(println)
    //printf("Average Error: %.2f\n", averageError(comparisons.toList))
    //printf("Average Percent Error: %.2f\n", averagePctError(comparisons.toList))
  }

  def kNN(toPredict : List[Double], vals : RDD[List[Double]], nNeighbors: Int = K) : Int = {
    val nearest = vals.map(entry => (calcDistance(toPredict, entry.tail), entry.head))
      .sortByKey(ascending = true)
      .take(nNeighbors).map({ case (_, price : Double) => price.toInt })
    getMode(nearest)
  }

  def getMode(l : Array[Int]) : Int = {
    val occurences = l.foldLeft(mutable.HashMap.empty[Int, Int].withDefaultValue(0))((acc, num) => {acc(num) += 1; acc}).toMap
    occurences.keys.toList.map(n => (occurences(n), n)).sortBy(_._1)(Ordering[Int].reverse).take(1).head._2
  }

  def calcDistance(p1 : List[Double], p2 : List[Double]) : Double = {
    val squared = p1.zip(p2).map(z => math.pow(z._1 - z._2, 2)).sum
    math.sqrt(squared)
  }


  def calcWindow(price : Double) : Int = {
    val width = (priceMax - priceMin) / numberOfWindows
    Math.floor((price - priceMin) / width).toInt
  }


  def normalize(entry : List[Double]) : List[Double] = {
    var retArray = mutable.MutableList[Double](calcWindow(entry.head))
    val minMaxArray = List((yearMin, yearMax), (0.0, 5.0), (0.0, 2.0), (0.0, 1.0), (odomMin, odomMax), (0.0, 2.0), (0.0, 1.0))


    for (i <- 1 to 7) {
      retArray = retArray :+ ((entry(i) - minMaxArray(i-1)._1) / (minMaxArray(i-1)._2 - minMaxArray(i-1)._1))
    }

    return retArray.toList
  }

  def metrics(sp: SparkContext, expected : List[Int], actual : List[Int]) : Unit = {
    val recall = calcRecall(sp.parallelize(expected), sp.parallelize(actual))
    val precicision = calcPrecision(expected, actual)
    val f1 = 2 / (1 / recall + 1 / precicision)
    System.out.println("Recall: %f\n Precision: %f\n F1 Score: %f".format(recall, precicision, f1))
  }

  def calcRecall(expected : RDD[Int], actual : RDD[Int]) : Double = {
    val buckets = expected.countByValue()
    val predicted = actual.countByValue()
    val agg = buckets.zip(predicted).map(x => Math.min(x._2._2, x._1._2) / x._1._2)
      .foldLeft((0, 0))((acc : (Int, Int), x : Long) => (acc._1 + x.toInt, acc._2 + 1))
    agg._1 * 1.0 / agg._2;
  }

  def calcPrecision(expected : List[Int], actual : List[Int]) : Double = {
    val selected = mutable.MutableList.fill(numberOfWindows)(0)
    val relevant = mutable.MutableList.fill(numberOfWindows)(0)
    actual.zip(expected).foreach(x => {
      if (x._1 == x._2) {
        relevant(x._1) += 1
      }
      selected(x._1) += 1
    })
    val agg = relevant.filter(_ != 0).zip(selected).reduce((acc, x) => (acc._1 + x._1, acc._2 + x._2))
    agg._1 * 1.0 / agg._2
  }

  def averageError(expectedVsActual: List[(Double, Double)]): Double ={
    val errors = expectedVsActual.map({case (exp, act) => ((exp - act).abs, 1)})
    val sumCount = errors.reduce((totErr, nextErr) => (totErr._1 + nextErr._1, totErr._2 + nextErr._2))
    sumCount._1 / sumCount._2
  }

  def averagePctError(expectedVsActual: List[(Double, Double)]): Double ={
    val errors = expectedVsActual.map({case (exp, act) => ((exp - act).abs / exp, 1)})
    val sumCount = errors.reduce((totErr, nextErr) => (totErr._1 + nextErr._1, totErr._2 + nextErr._2))
    sumCount._1 / sumCount._2 * 100
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

        val num = a.toString.toDouble


        if (i == 0) {
          priceMax = Math.max(priceMax, num)
          priceMin = Math.min(priceMin, num)
        }

        if (i == 1) {
          if (num > yearMax)
            yearMax = num
          else if (num < yearMin)
            yearMin = num
        }

        else if (i == 5) {
          if (num > odomMax)
            odomMax = num
          else if (num < odomMin)
            odomMin = num
        }
        
        retArray = retArray :+ a.toString.toDouble

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
