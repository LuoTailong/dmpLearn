package com.itheima.dmp.tools

import java.util.Date

import org.apache.commons.lang.time.FastDateFormat

object TimeUtils {
  //ODS20190106
  def getTime():String = {
    val fastDateFormat = FastDateFormat.getInstance("yyyyMMdd")
    val date = new Date
    val time: String = fastDateFormat.format(date)
    time
  }

  def main(args: Array[String]): Unit = {
    println(getTime())
  }
}
