import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.io.Source
import scala.reflect.io.Path

object testScala {
  implicit val rate1 = 9F

  implicit class JiSuan(x: Double) {
    def add(a: Int): Int = a + 1
  }

  implicit def double2int11(d: Float) = d.toInt

  def calcTax(amount: Float)(implicit rate: Float): Float = amount * rate

  class Triple[F: ClassTag, S, T](val first: F, val second: S, val third: T)

  def getData[T](list: List[T]) = list(list.length / 2)

  class Father(val name: String)

  class Child(name: String) extends Father(name)

  def setName[T <: Child](name: T) = {
    println("hashCode=" + name.hashCode())
    println(name)
  }

  private val DIR = "D:\\Demo"

  def main(args: Array[String]): Unit = {
//    testAggregate()
    val p = s"partition:${100}"
    println(p)
    return

    readFile()
    setName(new Child("father"))
    //    setName(100)
    val triple = new Triple("Spark", 3, 3.1415)
    println(triple.first)
    val bigData = new Triple[String, String, Char]("Spark", "Hadoop", 'R');
    println(getData(List("Spark", "Hadoop", 'R')))
    println(2.add(3))
    val tax = calcTax(100f)
    println(tax)
  }

  def readFile(): Unit = {
    val files = Path(this.DIR).walkFilter(p => p.isFile && p.name.contains("test"))
    for (file <- files) {
      val reader = Source.fromFile(file.toString(), "UTF-8")
      for (line <- reader.getLines()) {
        println(line)
      }
    }
  }



  def testAggregate(): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val sparkConf = new SparkConf().setAppName("test")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val list = List(1, 2, 3, 4, 6)
    val r = list.par.aggregate(1)((x, number) => x + number, (a, b) => (a + b))
    print("aggregate result:" + r)

    val data = List((1, 3), (1, 2), (1, 4), (2, 3), (1, 5))
    val rdd = sc.parallelize(data, 4)
    def combOp(a: String, b: String): String = {
      println("combOp: " + a + "\t" + b)
      a + b
    }

    //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
    def seqOp(a: String, b: Int): String = {
      println("SeqOp:" + a + "\t" + b)
      a + b
    }

    val r3 = rdd.aggregateByKey("100")(seqOp, combOp)
    println("aggregateByKey result:")
    r3.collect().foreach(println)
    //zeroValue:中立值,定义返回value的类型，并参与运算
    //seqOp:用来在同一个partition中合并值
    //combOp:用来在不同partiton中合并值
    val res: RDD[(Int, Int)] = rdd.aggregateByKey(100)(
      // seqOp
      math.min(_, _),
      // combOp
      _ + _)
    res.collect.foreach(println)
    sc.stop()
  }
}
