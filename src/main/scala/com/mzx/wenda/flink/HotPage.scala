package com.mzx.wenda.flink

import java.lang
import java.util.Properties

import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON

case class RequestInfo(timest: Long, event: String, reqUri: String, ip: String, userId: Int)

case class UrlCountView(url: String, windend: Long, count: Long)

object HotPage {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val REDIS_HOST = sys.env("REDIS_HOST")
    val KAFKA_HOST = sys.env("KAFKA_HOST")
    env.setParallelism(1)
    val topic = "req_log"

    // 1.0 配置KafkaConsumer
    val props = new Properties();

    props.setProperty("bootstrap.servers", KAFKA_HOST + ":9092")
    props.setProperty("group.id", "LoginFail")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val inputStream = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), props))
    //    inputStream.print("input")
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val formatedStream: DataStream[RequestInfo] = inputStream.flatMap(new FlatMapFunction[String, RequestInfo] {
      override def flatMap(value: String, out: Collector[RequestInfo]): Unit = {
        try {
          val array = value.split(",")
          val a = RequestInfo(array(0).trim.toLong, array(1).split(":")(1).trim, array(2).split(":")(1).trim, array(3).split(":")(1).trim, array(4).split(":")(1).trim.toInt)
          out.collect(a)
        } catch {
          case e: Exception => {
            System.err.println("can not convert")
          }
        }
      }
    }
    ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[RequestInfo](Time.seconds(1)) {
      override def extractTimestamp(element: RequestInfo): Long = element.timest
    })

    //    formatedStream.print("formated stream")
    val countStream = formatedStream.keyBy(_.reqUri)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .allowedLateness(Time.seconds(1))
      .aggregate(new HotPageAgg(), new HotPageCount())
    countStream.print("count")

    val conf = new FlinkJedisPoolConfig.Builder().setHost(REDIS_HOST).setPort(6379).build()
    countStream.keyBy(_.windend)
      .process(new TopKUrl(2)).addSink(new RedisSink[String](conf, new MyRedisMapper))

    env.execute("web_log")


  }

}

class HotPageAgg() extends AggregateFunction[RequestInfo, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(in: RequestInfo, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class HotPageCount() extends WindowFunction[Long, UrlCountView, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlCountView]): Unit = {

    out.collect(UrlCountView(key, window.getEnd, input.iterator.next()))

  }
}


class TopKUrl(topSize: Int) extends KeyedProcessFunction[Long, UrlCountView, String] {

  lazy val urlState: ListState[UrlCountView] = getRuntimeContext.getListState(new ListStateDescriptor[UrlCountView]("url-state2", classOf[UrlCountView]))

  override def processElement(i: UrlCountView, context: KeyedProcessFunction[Long, UrlCountView, String]#Context, collector: Collector[String]): Unit = {
    urlState.add(i)
    context.timerService().registerEventTimeTimer(i.windend + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlCountView, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlView: ListBuffer[UrlCountView] = new ListBuffer[UrlCountView]

    val iter = urlState.get().iterator()
    while (iter.hasNext) {
      allUrlView.append(iter.next())
    }

    urlState.clear()

    val sortedUrls = allUrlView.sortWith(_.count > _.count).take(topSize)

    val result: StringBuilder = new StringBuilder
    for (i <- sortedUrls.indices) {
      val url = sortedUrls(i)
      result.append(url.url)
      result.append("#-#")
    }
    out.collect(result.toString)
  }
}

class MyRedisMapper extends RedisMapper[String] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(t: String): String = "hoturl"

  override def getValueFromData(t: String): String = t
}