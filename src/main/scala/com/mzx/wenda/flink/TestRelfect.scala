package com.mzx.wenda.flink

import com.mzx.wenda.flink.dynamicrule.FieldsExtractor
import com.mzx.wenda.flink.view.Pojo

object TestRelfect {
  def main(args: Array[String]): Unit = {
    val ob = Pojo("12345")
    val res=FieldsExtractor.getFieldAsString(ob, "userId")
    println(res)

  }


}
