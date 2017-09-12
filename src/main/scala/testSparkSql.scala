import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object testSparkSql {
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("test")
    sparkConf.setMaster("local[*]");
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    val path = "d:\\demo\\people.json"
    val df = spark.read.json(path)
    df.printSchema()
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

    spark.udf.register("mylength", (input: String) => input.length)

    df.createOrReplaceTempView("people")

    val sqldf = spark.sql("select name,mylength(name) as length from people group by name")
    sqldf.show(5)

    sqldf.show()
  }
}
