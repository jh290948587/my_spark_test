package operator

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object GroupByKeyTest {
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

    val grouped = wordAndOne.groupByKey(6)
//    grouped.saveAsTextFile("out-group")

//    使用ShuffledRDD实现跟groupByKey效果一样的功能
    val shuffedRDD = new ShuffledRDD[String, Int, ArrayBuffer[Int]](wordAndOne
      , new HashPartitioner(wordAndOne.partitions.length))
//    分组不能在map side合并
    shuffedRDD.setMapSideCombine(false)

//    前两个函数都是在map side执行的
//    创建一个Combiner：就是将每一个组内的第一个value放到ArrayBuffer初始化
    val createCombiner = (v: Int) => ArrayBuffer(v)
//    将组内的其他value依次遍历追加到同一个ArrayBuffer
    val mergeValue = (buf: ArrayBuffer[Int], v: Int) => buf += v
//    第三个函数是在全局合并后执行的，是在shuffleRead之后
    val mergeCombiners = (c1: ArrayBuffer[Int], c2: ArrayBuffer[Int]) => c1 ++= c2

    shuffedRDD.setAggregator(new Aggregator[String, Int, ArrayBuffer[Int]](createCombiner
      , mergeValue, mergeCombiners))

    shuffedRDD.saveAsTextFile("my-out-group")
//    Thread.sleep(1000000000)
    sc.stop()

  }

}
