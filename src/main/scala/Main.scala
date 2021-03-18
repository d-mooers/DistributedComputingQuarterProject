import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._
import org.apache.spark

object Main {
  val INPUT_FILE = "data/vehicles.csv"
  val TESTING_NUM = 1000.0 // Number of entries to test on, or change equation in line 35 to a decimal for a percentage to train on
  val TRAIN_SPLIT = 0.8
  val OUTLIER_FLOOR = 1000
  val OUTLIER_CEILING = 200000
  val K = 105
  var yearMin = 3000.0
  var yearMax = 0.0
  var odomMin = 100000.0
  var odomMax = 0.0

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis();
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("asgn").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
    val lineItems = sc.textFile(INPUT_FILE).flatMap(_.split("\n")).map(_.split(","));

    val cleaned = lineItems.map(x => x.slice(5,7) ++ x.slice(9, 15)).map(entry => clean(entry)).filter(x => x.length == 8 && x(0) > OUTLIER_FLOOR && x(0) < OUTLIER_CEILING).persist()

    val normalized = cleaned.map(entry => normalize(entry))

    val training_rdd = normalized.sample(withReplacement = false, 1 - (TESTING_NUM / normalized.count())).persist()
    println(training_rdd.count())
    val cors = getCors(training_rdd);
    val test = normalized.subtract(training_rdd).collect()

    val correctPrice = test.map(_.head)
    // Need the extra () because K is provided in the second set of params.
    // If no K, then default K is used
    val predictedPrice = test.map(e => weightedKNN(e.tail, training_rdd)(nNeighbors = K))

    val comparisons = correctPrice.zip(predictedPrice)
    comparisons.foreach(println)
    printf("Average Error: %.2f\n", Util.averageError(comparisons.toList))
    printf("Average Percent Error: %.2f\n", Util.averagePctError(comparisons.toList))
    printf("Runtime(seconds): " + (System.currentTimeMillis() - startTime) / 1000)
  }

  // Default value of K is floor(sqrt(N))
  // Correlations default to all ones
  def kNN(toPredict : List[Double], vals : RDD[List[Double]])
     (nNeighbors: Int = Math.sqrt(vals.count).round.toInt,
      correlations: List[Double] = List.fill(toPredict.size)(1)) : Double = {
    vals.map(entry => (calcWeightedDistance(toPredict, entry.tail, correlations), entry.head))
      .sortByKey(ascending = true)
      .take(nNeighbors).map({ case (_, price) => price }).sum / nNeighbors
  }

  // Default value of K is floor(sqrt(N))
  // Correlations default to all ones
  def weightedKNN(toPredict : List[Double], vals : RDD[List[Double]])
                 (nNeighbors: Int = Math.sqrt(vals.count).round.toInt,
                  correlations: List[Double] = List.fill(toPredict.size)(1)) : Double = {
    val neighborRanks = (0 to (nNeighbors - 1)).toList;
    val invertedRanks = neighborRanks.map(ranking => nNeighbors - ranking)
    val invertedWeights = invertedRanks.map(invRank => invRank / invertedRanks.sum.toDouble)
    val kPrices = vals.map(entry => (calcWeightedDistance(toPredict, entry.tail, correlations), entry.head))
        .sortByKey(ascending = true)
        .take(nNeighbors).map({ case (_, price) => price })
    invertedWeights.zip(kPrices).map({case (weight, price) => weight * price}).sum
  }

  def calcDistance(p1 : List[Double], p2 : List[Double]) : Double = {
    val squared = p1.zip(p2).map(z => math.pow(z._1 - z._2, 2)).sum
    math.sqrt(squared)
  }

  def calcWeightedDistance(p1 : List[Double], p2 : List[Double], weights: List[Double]) : Double = {
    val allThree = p1.zip(p2).zip(weights).map({case ((p1, p2), weight) => (p1, p2, weight)});
    val weightedSquaredSum = allThree.map({case (p1, p2, weight) => math.pow(p1 - p2, 2) * weight}).sum
    math.sqrt(weightedSquaredSum)
  }


  def getCors(data: RDD[List[Double]]): List[Double]={
    val prices = data.map(record => record.head).persist()
    val avgPrices = Util.computeAverage(prices)
    val attrs = data.map(record => record.tail.toArray)
    val cors = new Array[Double](attrs.take(1)(0).size)
    for (i <- 0 to attrs.take(1)(0).size - 1){
      val curAttr = attrs.map(record => record(i))
      cors(i) = Util.correlation(curAttr, prices)(yBar = avgPrices)
    }
    cors.toList
  }


  def normalize(entry : List[Double]) : List[Double] = {
    var retArray = mutable.MutableList[Double](entry(0))
    val minMaxArray = List((yearMin, yearMax), (0.0, 5.0), (0.0, 2.0), (0.0, 1.0), (odomMin, odomMax), (0.0, 2.0), (0.0, 1.0))


    for (i <- 1 to 7) {
      retArray = retArray :+ ((entry(i) - minMaxArray(i-1)._1) / (minMaxArray(i-1)._2 - minMaxArray(i-1)._1))
    }

    return retArray.toList
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
