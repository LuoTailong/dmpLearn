package com.itheima.dmp.tag

import org.apache.spark.sql.Row
import java.util

object UserIds {
  def getUserId(row:Row):util.LinkedList[String] = {
    var linkedList = new util.LinkedList[String]()
    if (row.getAs[String]("imer").nonEmpty){
      linkedList.add("IMEI:"+row.getAs[String]("imei"))
    }
    if(row.getAs[String]("imeimd5").nonEmpty){
      linkedList.add("IMEIMD5:"+row.getAs[String]("imeimd5"))
    }
    if(row.getAs[String]("imeisha1").nonEmpty){
      linkedList.add("IMEISHA1:"+row.getAs[String]("imeisha1"))
    }

    //mac
    if(row.getAs[String]("mac").nonEmpty){
      linkedList.add("MAC:"+row.getAs[String]("mac"))
    }
    if(row.getAs[String]("macmd5").nonEmpty){
      linkedList.add("MACMD5:"+row.getAs[String]("macmd5"))
    }
    if(row.getAs[String]("macsha1").nonEmpty){
      linkedList.add("MACSHA1:"+row.getAs[String]("macsha1"))
    }
    //idfa
    if(row.getAs[String]("idfa").nonEmpty){
      linkedList.add("IDFA:"+row.getAs[String]("idfa"))
    }
    if(row.getAs[String]("idfamd5").nonEmpty){
      linkedList.add("IDFAMD5:"+row.getAs[String]("idfamd5"))
    }
    if(row.getAs[String]("idfasha1").nonEmpty){
      linkedList.add("IDFASHA1:"+row.getAs[String]("idfasha1"))
    }
    //openudid
    if(row.getAs[String]("openudid").nonEmpty){
      linkedList.add("OPENUDID:"+row.getAs[String]("openudid"))
    }
    if(row.getAs[String]("openudidmd5").nonEmpty){
      linkedList.add("OPENUDIDMD5:"+row.getAs[String]("openudidmd5"))
    }
    if(row.getAs[String]("openudidsha1").nonEmpty){
      linkedList.add("OPENUDIDSHA1:"+row.getAs[String]("openudidsha1"))
    }
    //androidid
    if(row.getAs[String]("androidid").nonEmpty){
      linkedList.add("ANDROIDID:"+row.getAs[String]("androidid"))
    }
    if(row.getAs[String]("androididmd5").nonEmpty){
      linkedList.add("ANDROIDIDMD5:"+row.getAs[String]("androididmd5"))
    }
    if(row.getAs[String]("androididsha1").nonEmpty){
      linkedList.add("ANDROIDIDSHA1:"+row.getAs[String]("androididsha1"))
    }
    linkedList
  }
}
