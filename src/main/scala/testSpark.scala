import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}


object testSpark {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext

    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 3, 5, 7, 9, 10, 34, 56, 78, 19, 20, 1, 3, 1, 5, 1, 7, 1, 8, 1, 9)
    val rdd = sc.parallelize(list)
    val sampleRdd = rdd.sample(false, 0.1)
    rdd.map(x => (x, 1)).reduceByKey(_ + _).collect().foreach(x => {
      //      println(x._1 + ":" + x._2)
    })
    println("groupByKey")
    rdd.map(x => (x, 1)).groupByKey().map(t => (t._1, t._2.sum)).collect().foreach(x => {
      //      println(x._1 + ":" + x._2)
    })

    val pp = for {i <- list} yield (i, 1)


    val rdd1 = sc.parallelize(Array(("A", 1), ("A", 2), ("B", 1), ("B", 2), ("C", 1), ("C", 2))).partitionBy(new HashPartitioner(2))
    rdd1.combineByKey(
      (v: Int) => v + "_",
      (c: String, v: Int) => c + "@" + v,
      (c1: String, c2: String) => c1 + c2
    ).collect().foreach(println)

    val mapRdd = sampleRdd.map(x => (x, 1))
    val countedRdd = mapRdd.reduceByKey((x, y) => x + y)
    println("countedRdd")
    countedRdd.collect().foreach(println)
    val reverseRdd = countedRdd.map(x => (x._2, x._1))
    println("reverseRdd")
    reverseRdd.collect().foreach(println)

    val r = reverseRdd.sortByKey(false).take(3).toList
    println("skewed key:")
    for (x <- r) {
      println(x._2)
    }
    return

    val result = sampleRdd.collect()
    for (x <- result) {
      println(x)
    }
  }
}
