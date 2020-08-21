package com.mzx.wenda.flink

import java.util
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import redis.clients.jedis.Jedis


object LoginFail {

  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
  case class LoginWarning(userId: Long, FailIp: String, lastFailTime: Long, msg: String)


  // envent:login,requestUri:/login,ip:172.30.48.1,username:123,fail
  def main(args: Array[String]): Unit = {

    val topic = "login_log"
    val REDIS_HOST = sys.env("REDIS_HOST")
    val KAFKA_HOST = sys.env("KAFKA_HOST")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.0 配置KafkaConsumer
    val props = new Properties();
    props.setProperty("bootstrap.servers", KAFKA_HOST + ":9092")
    props.setProperty("group.id", "LoginFail")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val inputStream = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props)).map(
      line => {
        val dataArray = line.split(",")
        LoginEvent(dataArray(4).split(":")(1).toLong,
          dataArray(3).split(":")(1),
          dataArray(5),
          dataArray(0).toLong)
      }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime
    })


    val LoginFailP = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("second")
      .where(_.eventType == "fail")
      .next("third")
      .where(_.eventType == "fail")
      .within(Time.seconds(30))

    val PatternStream = CEP.pattern(inputStream.keyBy(_.userId), LoginFailP)

    val warningStream = PatternStream.select(new PatternSelectFunction[LoginEvent, LoginWarning] {
      override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarning = {
        val logenv = map.get("third").iterator().next()
        LoginWarning(logenv.userId, logenv.ip, logenv.eventTime, "登陆失败次数过多")
      }
    })

//    val conf = new FlinkJedisPoolConfig.Builder().setHost(REDIS_HOST).setPort(6379).build()

    warningStream.addSink(new CustomSinkToRedis(REDIS_HOST))
    warningStream.print("log fail")
    env.execute("log fail")
  }

  class CustomSinkToRedis(redis_host: String) extends RichSinkFunction[LoginWarning] {
    var redisCon: Jedis = _

    override def open(parameters: Configuration): Unit = {
      this.redisCon = new Jedis(redis_host)
    }

    override def close(): Unit = {
      this.redisCon.close()
    }

    override def invoke(value: LoginWarning, context: SinkFunction.Context[_]): Unit = {
      val key = "logfail" + value.userId.toString
      redisCon.set(key, "yes")
      if (this.redisCon.ttl(key) == -1) {
        this.redisCon.expire(key, 60 * 2)
      }

    }

  }


}
