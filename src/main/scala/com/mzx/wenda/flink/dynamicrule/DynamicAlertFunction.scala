package com.mzx.wenda.flink.dynamicrule

import org.apache.flink.api.common.state._
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
case class LoginWarning(userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String)
class DynamicAlertFunction extends KeyedBroadcastProcessFunction[String, Keyed[LoginEvent, String, Int], Rule, LoginWarning] {

  val logger = LoggerFactory.getLogger("alert")
  val RULES_STATE_DESCRIPTOR = new MapStateDescriptor[Int, Rule]("ruls", classOf[Int], classOf[Rule])
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))
  lazy val maxTimes: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("maxLoginFailTimes", classOf[Int]))

  override def processElement(value: Keyed[LoginEvent, String, Int], readOnlyContext: KeyedBroadcastProcessFunction[String, Keyed[LoginEvent, String, Int], Rule, LoginWarning]#ReadOnlyContext, collector: Collector[LoginWarning]): Unit = {
    val event = value.getWrapped
    val rule: Rule = readOnlyContext.getBroadcastState(RULES_STATE_DESCRIPTOR).get(value.getId)
    val loginFialList = loginFailState.get()

    logger.info("当前watermarker  " + readOnlyContext.timerService().currentWatermark())
    if (event.getEventType == "fail") {
      //      第一次失败，注册定时器
      if (!loginFialList.iterator().hasNext) {
        val ts = event.getEventTime + rule.getDuration * 1000
        readOnlyContext.timerService().registerEventTimeTimer(ts)
        logger.info("注册定时器 时间戳为 " + ts)
        maxTimes.update(rule.getMaxTimes)
      }
      loginFailState.add(event)
    } else {
      loginFailState.clear()
    }
  }


  override def processBroadcastElement(rule: Rule, context: KeyedBroadcastProcessFunction[String, Keyed[LoginEvent, String, Int], Rule, LoginWarning]#Context, collector: Collector[LoginWarning]): Unit = {
    val broadcastState = context.getBroadcastState(RULES_STATE_DESCRIPTOR)
    logger.info("time on")
    if (rule.getRuleState == "update") {
      broadcastState.put(rule.getRuleId, rule)
    } else if (rule.getRuleState == "delete") {
      broadcastState.remove(rule.getRuleId)
    } else if (rule.getRuleState == "add") {
      broadcastState.put(rule.getRuleId, rule)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[String, Keyed[LoginEvent, String, Int], Rule, LoginWarning]#OnTimerContext, out: Collector[LoginWarning]): Unit = {
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailState.get().iterator()
    while (iter.hasNext) {
      allLoginFails += iter.next()
    }
    val times = maxTimes.value()
    if (allLoginFails.length > times) {
      out.collect(LoginWarning(allLoginFails.head.getUserId, allLoginFails.head.getEventTime, allLoginFails.last.getEventTime, "loginfail times:" + allLoginFails.length))
    }
  }
}
