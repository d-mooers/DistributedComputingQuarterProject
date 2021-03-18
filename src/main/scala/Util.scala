import org.apache.spark.rdd.RDD

object Util {

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

  def correlation(x: RDD[Double], y: RDD[Double])
                 (xBar: Double = computeAverage(x), yBar:Double= computeAverage(y)): Double={
    val numerator = x.zip(y).map({case (xVal, yVal) => (xVal - xBar) * (yVal - yBar)}).sum
    val leftDenom = Math.sqrt(x.map(xVal => Math.pow(xVal - xBar, 2)).sum)
    val rightDenom = Math.sqrt(y.map(yVal => Math.pow(yVal - yBar, 2)).sum)
    numerator / (leftDenom * rightDenom)
  }

  def computeAverage(x: List[Double]): Double ={
    val sumCount = x.map(xVal => (xVal, 1)).reduce((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2 ))
    sumCount._1 / sumCount._2
  }

  def computeAverage(x: RDD[Double]): Double ={
    val start = (0.0, 0)
    val combiner = (agg: (Double, Int), value: Double) => (agg._1 + value, agg._2 + 1)
    val merger = (agg1: (Double, Int), agg2: (Double, Int)) => (agg1._1 + agg2._1, agg1._2 + agg2._2)
    val sumCount = x.aggregate(start)(combiner, merger)
    sumCount._1 / sumCount._2
  }

}
