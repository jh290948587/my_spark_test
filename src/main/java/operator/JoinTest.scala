package operator

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("join_demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("hello",9),("world",8),("spark",3),("flink",4),("spark",9)))
    val rdd2 = sc.parallelize(List(("spark",6),("spark",2),("spark",5),("flink",7),("flink",12)))
    val rdd3 = rdd1.join(rdd2)
    print("join:" + rdd3.collect().toBuffer)
    val rdd4 = rdd1.cogroup(rdd2)
    print("cogroup:" + rdd4.collect().toBuffer)
    val rdd5 = rdd4.mapValues(values => for( v1 <- values._1; v2 <- values._2) yield (v1, v2))
    print("mapValues:" + rdd5.collect().toBuffer)
    val rdd6 = rdd4.flatMapValues(values => for( v1 <- values._1.iterator; v2 <- values._2.iterator) yield (v1, v2))
    print("flatMapValues:" + rdd6.collect().toBuffer)
//    Thread.sleep(1000000000)
    sc.stop()

  }

}
