package kafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
//    getKafkaPartitions()
    consumeKafkaMsg()
//        consumeKafkaMsg_V1()
//        getKafkaBrokers()
  }

  def getKafkaBrokers(): Unit = {
    val zk = new ZooKeeper("kjtlxsvr4:2181", 10000, null)
    val ids = zk.getChildren("/brokers/ids", false)
    for (id <- ids) {
      val brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null))
      System.out.println(id + ": " + brokerInfo)
    }
  }

  def getKafkaPartitions(): Unit = {
    val zk = new ZooKeeper("kjtlxsvr4:2181", 10000, null)
    val ids = zk.getChildren("/brokers/topics", false)
    for (id <- ids) {
      val brokerInfo = new String(zk.getData("/brokers/topics/" + id, false, null))
      System.out.println(id + ": " + brokerInfo)
    }
  }

  def consumeKafkaMsg_V1(): Unit = {
    import kafka.consumer.{Consumer, ConsumerConfig}

    val topic = "test-kafka"
    val consumerid = "consumer"
    val groupid = "something"
    val clientid = "test"

    val props = new Properties()
    props.put("zookeeper.connect", "kjtlxsvr4:2181")
    props.put("group.id", groupid)
    props.put("client.id", clientid)
    props.put("consumer.id", consumerid)
    props.put("auto.offset.reset", "smallest")
    props.put("auto.commit.enable", "true")
    props.put("auto.commit.interval.ms", "100")

    val consumerConfig = new ConsumerConfig(props)
    val consumer = Consumer.create(consumerConfig)

    val topicCountMap = Map(topic -> 1)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get
    for (stream <- streams) {
      val it = stream.iterator()
      while (it.hasNext()) {
        val messageAndMetadata = it.next()
        val message = s"Topic:${messageAndMetadata.topic}, GroupID:$groupid, Consumer ID:$consumerid, PartitionID:${messageAndMetadata.partition}, " +
          s"Offset:${messageAndMetadata.offset}, Message Payload: ${new String(messageAndMetadata.message())}"
        System.out.println(message);
      }
    }
  }


  def consumeKafkaMsg(): Unit = {
    val TOPIC = "test-kafka"
    val props = new Properties()
    props.put("bootstrap.servers", "kjtlxsvr6:9092,kjtlxsvr6:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList(TOPIC))
    while (true) {
      val records = consumer.poll(100)
      for (record <- records.asScala) {
        val p = s"partition:${record.partition()}"
        println(p)

        println(record)
      }
      Thread.sleep(100)
    }
  }
}
