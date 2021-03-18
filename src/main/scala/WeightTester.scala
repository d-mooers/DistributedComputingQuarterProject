import Main.{INPUT_FILE, OUTLIER_CEILING, OUTLIER_FLOOR, TESTING_NUM, clean, getCors, kNN, normalize, weightedKNN}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.io.FileWriter
import scala.collection.immutable.HashMap

object WeightTester {

  val N_TRIALS = 3;
  val OUT_PATH = "tests/weightTest.csv"

  def main(args: Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("WeightTest").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lineItems = sc.textFile(INPUT_FILE).flatMap(_.split("\n")).map(_.split(","));
    val cleaned = lineItems.map(x => x.slice(5,7) ++ x.slice(9, 15)).map(entry => clean(entry)).filter(x => x.length == 8 && x(0) > OUTLIER_FLOOR && x(0) < OUTLIER_CEILING).persist()
    val normalized = cleaned.map(entry => normalize(entry))
    val results = new Array[(String, Double, Double)](N_TRIALS * 4)
    println("Running Tests")
    for (i <- 0 to N_TRIALS - 1){
      results(i * 4) = runNormal(normalized)
      results(i * 4 + 1) = runWeighted(normalized)
      results(i * 4 + 2) = runNormalCor(normalized)
      results(i * 4 + 3) = runWeightedCor(normalized)
    }
    writeResults(results, List("Test", "Average Error", "Average Percent Error"), OUT_PATH);
  }

  // No Weights
  def runNormal(normalized: RDD[List[Double]]): (String, Double, Double) ={
    val training_rdd = normalized.sample(withReplacement = false, 1 - (TESTING_NUM / normalized.count())).persist()
    val test = normalized.subtract(training_rdd).collect()
    val correctPrice = test.map(_.head)
    val predictedPrice = test.map(e => kNN(e.tail, training_rdd)())
    val comparisons = correctPrice.zip(predictedPrice)
    ("No Weights", Util.averageError(comparisons.toList), Util.averagePctError(comparisons.toList))
  }

  // Weights the results by position
  def runWeighted(normalized: RDD[List[Double]]): (String, Double, Double) ={
    val training_rdd = normalized.sample(withReplacement = false, 1 - (TESTING_NUM / normalized.count())).persist()
    val test = normalized.subtract(training_rdd).collect()
    val correctPrice = test.map(_.head)
    val predictedPrice = test.map(e => weightedKNN(e.tail, training_rdd)())
    val comparisons = correctPrice.zip(predictedPrice)
    ("Positional Weight", Util.averageError(comparisons.toList), Util.averagePctError(comparisons.toList))
  }

  // Weights the distances by correlation
  def runNormalCor(normalized: RDD[List[Double]]): (String, Double, Double) ={
    val training_rdd = normalized.sample(withReplacement = false, 1 - (TESTING_NUM / normalized.count())).persist()
    val test = normalized.subtract(training_rdd).collect()
    val correctPrice = test.map(_.head)
    val predictedPrice = test.map(e => kNN(e.tail, training_rdd)(correlations = getCors(training_rdd)))
    val comparisons = correctPrice.zip(predictedPrice)
    ("Correlation Weight", Util.averageError(comparisons.toList), Util.averagePctError(comparisons.toList))
  }

  // Weights the results by position
  // Weights the distances by correlation
  def runWeightedCor(normalized: RDD[List[Double]]): (String, Double, Double) ={
    val training_rdd = normalized.sample(withReplacement = false, 1 - (TESTING_NUM / normalized.count())).persist()
    val test = normalized.subtract(training_rdd).collect()
    val correctPrice = test.map(_.head)
    val predictedPrice = test.map(e => weightedKNN(e.tail, training_rdd)(correlations = getCors(training_rdd)))
    val comparisons = correctPrice.zip(predictedPrice)
    ("Correlation & Positional Weight", Util.averageError(comparisons.toList), Util.averagePctError(comparisons.toList))
  }

  /*
   * Write given list of list to given file path in csv format.
   */
  def writeResults(resultValues: Array[(String, Double, Double)], headers: List[String], path:String): Unit={
    if (headers.length != 3){
      println("Couldn't write results. Length of header must equal length of result row.")
      return
    }
    var sb = new StringBuilder;
    val fw = new FileWriter(path);

    sb.append(headers.mkString(","))
    sb.append("\n")
    sb.append(resultValues.map(row => row._1 + "," + row._2 + "," + row._3).mkString("\n"))
    fw.write(sb.toString())
    fw.close()
    println("Results successfully written to " + path)
  }
}
