import java.util.Properties

import org.apache.spark.sql.{SQLContext, SparkSession}

object testSparkSql {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df1 = spark.read.option("header", "true").csv("d:\\demo\\aa.csv")

    readMySqlData(spark.sqlContext)

    writeMySqlData(spark.sqlContext)

    //    readHiveData(spark.sqlContext)

    df1.createOrReplaceTempView("people")

    //    df1.printSchema()
    //    df1.select("name", "age").show(5)
    //    df1.show()
    //    spark.table("people").write.saveAsTable("testhive")
    //    spark.sql("select name,age from testhive").show()

    import spark.implicits._
    val path = "d:\\demo\\people.json"
    val df = spark.read.json(path)

    df.printSchema()
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

    spark.udf.register("mylength", (input: String) => input.length)
    spark.udf.register("wordcount", new MyUDAF)

    df.createOrReplaceTempView("people")

    val sqldf = spark.sql("select name,mylength(name) as length,wordcount(name) as count from people group by name")
    sqldf.show(5)

    sqldf.show()
  }

  def readHiveData(context: SQLContext): Unit = {
    context.sql("select * from kjt_pv limit 10").show()
  }

  def readMySqlData(sqlContext: SQLContext): Unit = {
    sqlContext.read.format("jdbc").options(Map("url" -> "jdbc:mysql://10.10.2.19:3306/piwik",
      //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "(select * from piwik_site) as site",
      "user" -> "bigdatateam", "password" -> "kjt.com")).load().show()
  }

  def writeMySqlData(sQLContext: SQLContext): Unit = {
    //将结果写入数据库中
    val path = "d:\\demo\\people.json"
    val df = sQLContext.read.json(path)
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "Kjt.com")
    df.write.mode("append").jdbc("jdbc:mysql://192.168.60.45:3306/kjt?useUnicode=true&characterEncoding=UTF-8", "product", properties)
  }

  import org.apache.spark.sql.expressions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.Row

  class MyUDAF extends UserDefinedAggregateFunction {
    // 该方法指定具体输入数据的类型
    override def inputSchema: StructType = StructType(Array(StructField("input", StringType, true)))

    //在进行聚合操作的时候所要处理的数据的结果的类型
    override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, true)))

    //指定UDAF函数计算后返回的结果类型
    override def dataType: DataType = IntegerType

    // 确保一致性 一般用true
    override def deterministic: Boolean = true

    //在Aggregate之前每组数据的初始化结果
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
    }

    // 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
    // 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Int](0) + 1
    }

    //最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    }

    //返回UDAF最后的计算结果
    override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
  }
}