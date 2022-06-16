package operator

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

object FoldByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("groupByKey_demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val wordsList = List(
      "spark", "hadoop", "hive", "spark",
      "spark", "flink", "spark", "hbase",
      "kafka", "kafka", "kafka", "kafka",
      "hadoop", "flink", "hive", "flink",
    )
    val words = sc.parallelize(wordsList, 4)
    val wordAndOne = words.map((_, 1))

    val folded = wordAndOne.foldByKey(10)(_ + _)

    folded.saveAsTextFile("foldByKey-out")
//    Thread.sleep(1000000000)
    sc.stop()

  }

}
