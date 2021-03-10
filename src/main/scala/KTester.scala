import Main.{INPUT_FILE, TRAINING_NUM, averageError, averagePctError, clean, kNN, normalize}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import java.io.FileWriter

/*
 * Running the main method of this class produces a csv file in the format K, Avg Error, PCT Error
 */
object KTester {
  val N_TRIALS = 3;
  val SMALL_K = 5;
  val BIG_K = 15;
  val OUT_PATH = "tests/kTest.csv"

  def main(args: Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("KTester").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lineItems = sc.textFile(INPUT_FILE).flatMap(_.split("\n")).map(_.split(","));
    val cleaned = lineItems.map(x => x.slice(5,7) ++ x.slice(9, 15)).map(entry => clean(entry)).filter(x => x.length == 8 && x(0) != 0).persist()
    val normalized = cleaned.map(entry => normalize(entry))
    val Ks = (SMALL_K to BIG_K toList).flatMap(curK => List.fill(N_TRIALS) (curK))
    println("Running Tests")
    val results = Ks.map(curK => runTest(normalized, curK))
    writeResults(results, List("K", "Average Error", "Average Percent Error"), OUT_PATH);
  }

  /*
   * Given an RDD containing Lists of doubles, each representing one record of normalized training data,
   * and the number of neighbors to test, return a three element list containing the number of neighbors tested,
   * the average error, and the percent error.
   */
  def runTest(normalized: RDD[List[Double]], nNeighbors: Int): List[Double]={
    val training_rdd = normalized.sample(withReplacement = false, 1 - (TRAINING_NUM / normalized.count())).persist()
    val test = normalized.subtract(training_rdd).collect()
    val correctPrice = test.map(_.head)
    val predictedPrice = test.map(e => kNN(e.tail, training_rdd, nNeighbors))
    val comparisons = correctPrice.zip(predictedPrice)
    List(nNeighbors, averageError(comparisons.toList), averagePctError(comparisons.toList))
  }

  /*
   * Write given list of list to given file path in csv format.
   */
  def writeResults(resultValues: List[List[Double]], headers: List[String], path:String): Unit={
    if (headers.length != resultValues.head.length){
      println("Couldn't write results. Length of header must equal length of result row.")
      return
    }
    var sb = new StringBuilder;
    val fw = new FileWriter(path);

    sb.append(headers.mkString(","))
    sb.append("\n")
    sb.append(resultValues.map(row => row.mkString(",")).mkString("\n"))
    fw.write(sb.toString())
    fw.close()
    println("Results successfully written to " + path)
  }
}