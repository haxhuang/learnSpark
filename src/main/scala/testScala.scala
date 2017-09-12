import scala.reflect.ClassTag

object testScala{
  implicit val rate1 = 9F

  implicit class JiSuan(x: Double) {
    def add(a: Int): Int = a + 1
  }

  implicit def double2int11(d: Float) = d.toInt

  def calcTax(amount: Float)(implicit rate: Float): Float = amount * rate

  class Triple[F: ClassTag, S, T](val first: F, val second: S, val third: T)

  def getData[T](list:List[T]) = list(list.length / 2)

  def main(args: Array[String]): Unit = {
    val triple = new Triple("Spark", 3, 3.1415)
    println(triple.first)
    val bigData = new Triple[String, String, Char]("Spark", "Hadoop", 'R');
    val list=List(1,2,3)
    print(list(1))
    println(getData(List("Spark", "Hadoop", 'R')))

    println(2.add(3))
    val tax = calcTax(100f)
    println(tax)
  }
}
