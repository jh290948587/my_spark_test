package operator

import org.apache.spark.{SparkConf, SparkContext}

object MapAndFilterTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map_demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("hello",9),("world",8),("spark",3),("flink",4)))
//    map: 需要的函数是，给定一个其中的元素，转换成另一元素
    val rdd2 = rdd1.map(_._2)
    val rdd3 = rdd2.filter(_ % 2 == 0)
//    mapPartitions: 需要的函数是，给定一个迭代器，转换成另一个迭代器，用户函数的参数是一个分区的迭代器
//    map和mapPartitions的区别就在于，computer方法会将分区迭代器传入，
    //                            ，map是分区迭代器遍历调用用户的函数
//                                ，mapPartitions是用户函数传入分区迭代器然后在用户函数里遍历分区迭代器
//    new MapPartitionsRDD[U, T](this, (_, _, iter) => iter.map(cleanF))
//    new MapPartitionsRDD(this, (_: TaskContext, _: Int, iter: Iterator[T]) => cleanedF(iter),
//        preservesPartitioning)
    val rdd4 = rdd3.mapPartitions(par => {
      par.map(x => x)
    })
//    mapPartitionsWithIndex: 预先传入了分区索引
    val rdd5 = rdd4.mapPartitionsWithIndex((index, iter) => {
      iter.map(s"index:$index, ele:" + _ )
    })
    print(rdd5.collect().toBuffer)
  }

}
