package com.mzx.wenda.flink.dynamicrule

import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

class DynamicKeyFunction extends BroadcastProcessFunction[LoginEvent, Rule, Keyed[LoginEvent, String, Int]] {
  val RULES_STATE_DESCRIPTOR = new MapStateDescriptor[Int, Rule]("ruls", classOf[Int], classOf[Rule])

  override def processElement(envent: LoginEvent, readOnlyContext: BroadcastProcessFunction[LoginEvent, Rule, Keyed[LoginEvent, String, Int]]#ReadOnlyContext, collector: Collector[Keyed[LoginEvent, String, Int]]): Unit = {
    val ruleState: ReadOnlyBroadcastState[Int, Rule] = readOnlyContext.getBroadcastState(RULES_STATE_DESCRIPTOR)
    val rules = ruleState.immutableEntries()
    val it = rules.iterator()
    while (it.hasNext) {
      val ruleEntry = it.next()
      collector.collect(new Keyed[LoginEvent, String, Int](envent,
        KeysExtractor.getKey(ruleEntry.getValue.getGroupingKeyNames, envent),
        ruleEntry.getValue.getRuleId))
    }
  }


  override def processBroadcastElement(rule: Rule, context: BroadcastProcessFunction[LoginEvent, Rule, Keyed[LoginEvent, String, Int]]#Context, collector: Collector[Keyed[LoginEvent, String, Int]]): Unit = {
    val broadcastState = context.getBroadcastState(RULES_STATE_DESCRIPTOR)
    if (rule.getRuleState == "update") {
      broadcastState.put(rule.getRuleId, rule)
    } else if (rule.getRuleState == "delete") {
      broadcastState.remove(rule.getRuleId)
    } else if (rule.getRuleState == "add") {
      broadcastState.put(rule.getRuleId, rule)
    }
  }
}
