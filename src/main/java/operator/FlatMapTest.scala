package operator

import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("flatmap_demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val arr = Array("spark hadoop flink spark", "spark hadoop flink", "spark hadoop hadoop")
    val lines = sc.parallelize(arr)
    val flattend = lines.flatMap(x => x.split(" "))

    print(flattend.collect().toBuffer)
  }

}
