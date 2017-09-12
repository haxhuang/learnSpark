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
}
