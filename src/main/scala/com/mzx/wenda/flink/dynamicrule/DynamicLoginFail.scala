package com.mzx.wenda.flink.dynamicrule

import java.util
import java.util.Properties

//import com.mzx.userbehavior.LogInAanalysis.LoginEvent
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.LoggerFactory

object DynamicLoginFail {
  val RULES_STATE_DESCRIPTOR = new MapStateDescriptor[Int, Rule]("ruls", classOf[Int], classOf[Rule])

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("loginFail")
    val log_topic = "login_log"
    val rule_topic = "login_rule"
    val REDIS_HOST = sys.env("REDIS_HOST")
    val KAFKA_HOST = sys.env("KAFKA_HOST")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 1.0 配置KafkaConsumer
    val props = new Properties();
    props.setProperty("bootstrap.servers", KAFKA_HOST + ":9092")
    props.setProperty("group.id", "LoginFail")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputStream = env.addSource(new FlinkKafkaConsumer[String](log_topic, new SimpleStringSchema(), props)).map(
      line => {
        val dataArray = line.split(",")
        new LoginEvent(dataArray(4).split(":")(1).toLong,
          dataArray(3).split(":")(1),
          dataArray(5),
          dataArray(0).toLong)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = {
        logger.info("抽取的时间戳 " + element.getEventTime)
        element.getEventTime
      }
    })

    val ruleUpdateStream: DataStream[Rule] = env.addSource(new FlinkKafkaConsumer[String](rule_topic, new SimpleStringSchema(), props))
      .map(
        line => {
          val dataArray = line.split(",")
          logger.info("new rule")

          val keys = new util.ArrayList[String]()
          val array = dataArray(2).replace("{", "").replace("}", "").split("#")
          for (i <- array.indices) {
            keys.add(array(i))
          }
          new Rule(
            dataArray(0).toInt,
            dataArray(1),
            keys,
            dataArray(3).toLong,
            dataArray(4).toInt)
        }
      ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Rule](Time.seconds(0)) {
      override def extractTimestamp(element: Rule): Long = Long.MaxValue
    })

    val ruleStream: BroadcastStream[Rule] = ruleUpdateStream.broadcast(RULES_STATE_DESCRIPTOR)
    val connectedStream = inputStream.connect(ruleStream)


    val alertStream = connectedStream.process(new DynamicKeyFunction)
      .keyBy(_.getKey)
      .connect(ruleStream)

      .process(new DynamicAlertFunction)

    alertStream.print("login_fail")
    //    inputStream.process(new DynamicKeyFunction)
    //      .keyBy(_.getKey)
    //      .process(new DynamicAlertFunction())

    env.execute("login_fail_dynamic")
  }

}
