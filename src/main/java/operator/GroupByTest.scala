package operator

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object GroupByTest {
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

    val grouped = wordAndOne.groupBy(_._1)
//    用groupByKey实现groupBy
    val grouped2 = wordAndOne.map(x => (x._1, x)).groupByKey()
    grouped2.saveAsTextFile("out-group-by")

//    Thread.sleep(1000000000)
    sc.stop()

  }

}
