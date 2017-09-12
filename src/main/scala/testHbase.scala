import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import scala.collection.JavaConversions._
import util.utility.timer

object testHbase {
  val tablename: String = "t2"
  val cf: String = "c1"
  val qulified: String = "c11"

  def main(args: Array[String]): Unit = {
    val HBASE_CONFIG = new Configuration();
    HBASE_CONFIG.set("hbase.zookeeper.quorum", "kjtlxsvr4,kjtlxsvr5,kjtlxsvr6");
    HBASE_CONFIG.set("hbase.zookeeper.property.clientPort", "2181");
    val conf = HBaseConfiguration.create(HBASE_CONFIG);
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin
    val userTable = TableName.valueOf(tablename)
    if (admin.tableExists(userTable)) {
      println("Table exists!")
      //admin.disableTable(userTable)
      //admin.deleteTable(userTable)
      //exit()
    } else {
      val tableDesc = new HTableDescriptor(userTable)
      tableDesc.addFamily(new HColumnDescriptor(cf.getBytes))
      admin.createTable(tableDesc)
      println("Create table success!")
    }

    val table = conn.getTable(userTable)
    testGet(table)
    //    testDelete(table)
    testScan(table)

    table.close()
    conn.close()
  }

  def testDelete(table: Table): Unit = {
    timer {
      val d = new Delete("id001".getBytes)
      d.addColumn(cf.getBytes, qulified.getBytes)
      table.delete(d)
    }
  }

  def testGet(table: Table): Unit = {
    timer {
      val g = new Get("00010730257914.5".getBytes)
      val result = table.get(g)
      val value = Bytes.toString(result.getValue(cf.getBytes, qulified.getBytes))
      println("GET by rowkey:" + value)
    }
  }

  def testScan(table: Table): Unit = {
    timer {
      val s = new Scan()
      s.setStartRow(Bytes.toBytes("00010"))
      s.setStopRow(Bytes.toBytes("00010730363610.4"))
      s.addColumn(cf.getBytes, qulified.getBytes)
      val scanner = table.getScanner(s)
      try {
        for (r <- scanner.iterator()) {
          println(cf + ":" + qulified + "=" + Bytes.toString(r.getValue(cf.getBytes, qulified.getBytes)))
        }
      } finally {
        scanner.close()
      }
    }
  }
}
