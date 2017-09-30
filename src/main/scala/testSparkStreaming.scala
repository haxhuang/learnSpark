import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object testSparkStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example1")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val ssc = new StreamingContext(sc, Seconds(2))
    val zkQuorum = "kjtlxsvr4:2181,kjtlxsvr5:2181,kjtlxsvr6:2181"
    val group = "something"
    val topics = "test-kafka1"
    val numThreads = 1
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    //    lineMap.map(_._2).print()
    val lines = lineMap.map(_._2)
    val words = lines.flatMap(_.split(","))
    val pair = words.map(x => (x, 1))
    //每隔2s钟统计前20s内的单词数
    new org.apache.spark.streaming.Duration(10)
    //    val wordCounts = pair.reduceByKeyAndWindow(_ + _, _ - _, Seconds(20), Seconds(2), 2)
    val wordCounts = pair.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(20), Seconds(2))
    wordCounts.print
//    ssc.checkpoint("d:\\demo\\checkpoint")
    ssc.start
    ssc.awaitTermination
  }
}
