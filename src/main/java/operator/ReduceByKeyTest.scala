package operator

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object ReduceByKeyTest {
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

    val reduced = wordAndOne.reduceByKey(_ + _)

//    使用groupByKey实现reduceByKey
    val grouped = wordAndOne.groupByKey()
    val reduced2 = grouped.mapValues(_.sum)


//    使用CombineByKey实现reduceByKey的功能
    val f1 = (x: Int) => x
    val f2 = (m: Int, n: Int) => m + n
    val f3 = (a: Int, b: Int) => a + b
    val reduced3 = wordAndOne.combineByKey(f1, f2, f3)

//    使用ShuffledRDD实现reduceByKey的功能
//    使用ShuffledRDD实现跟groupByKey效果一样的功能
//    shuffedRDD只是先将数据根据hash分区了
    val shuffedRDD = new ShuffledRDD[String, Int, Int](wordAndOne
      , new HashPartitioner(wordAndOne.partitions.length))
//    分组不能在map side合并
    shuffedRDD.setMapSideCombine(true)
    //    前两个函数都是在map side执行的
    //    创建一个Combiner：就是将每一个组内的第一个value放到ArrayBuffer初始化
    val createCombiner = (v: Int) => v
    //    将组内的其他value依次遍历追加到同一个ArrayBuffer
    val mergeValue = (m: Int, n: Int) => m + n
    //    第三个函数是在全局合并后执行的，是在shuffleRead之后
    val mergeCombiners = (a: Int, b: Int) => a + b

    shuffedRDD.setAggregator(new Aggregator[String, Int, Int](createCombiner
      , mergeValue, mergeCombiners))

    shuffedRDD.saveAsTextFile("reduceByKey-out")
//    Thread.sleep(1000000000)
    sc.stop()

  }

}
