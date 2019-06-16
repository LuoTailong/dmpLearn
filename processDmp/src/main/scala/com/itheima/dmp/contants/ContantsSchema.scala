package com.itheima.dmp.contants

import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import scala.collection.JavaConverters._

/**
  * Created by angel
  */
object ContantsSchema {
  //1：初始化
  lazy val odsSchema: Schema = {
    val columns = List(
      //nullable:是否允许为null
      //key：是否是主键
      new ColumnSchemaBuilder("ip", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("sessionid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("advertisersid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adorderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adcreativeid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformproviderid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("sdkversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("adplatformkey", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("putinmodeltype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("requestmode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adppprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("requestdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("client", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("osversion", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("density", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("pw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ph", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("long", Type.STRING).nullable(false).build(), //TODO
      new ColumnSchemaBuilder("lat", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("ispid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannerid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iseffective", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbilling", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adspacetypename", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("devicetype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("processnode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("apptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("district", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("paymode", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("isbid", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("winprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("iswin", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("cur", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("cnywinprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("imei", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mac", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfa", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androidid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbprovince", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbcity", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbdistrict", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("rtbstreet", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("storeurl", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("realip", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("isqualityapp", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidfloor", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("aw", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("ah", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("imeimd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfamd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididmd5", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("imeisha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("macsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("idfasha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("openudidsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("androididsha1", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("uuidunknow", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("userid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("iptype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("initbidprice", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adpayment", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("agentrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("lomarkrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adxrate", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("title", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("keywords", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tagid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("callbackdate", Type.STRING).nullable(true).build(),
      new ColumnSchemaBuilder("channelid", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("mediatype", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("email", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("tel", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("sex", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("age", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  lazy val regionSchema: Schema = {
    val columns = List(
      //nullable:是否允许为null
      //key：是否是主键
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("count", Type.INT64).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  /**
    * "select provincename , cityname , " +
    * "adOriginalRequestNum , " +
    * "avildRequestNum , " +
    * "adRequestNum , " +
    * "bidNum , " +
    * "bidSusNum , " +
    * "bidSusNum/bidNum as bidSusRid , " +
    * "adImproNum , " +
    * "adClickNum , " +
    * "mediaImproNum , " +
    * "mediaClickNum , " +
    * "mediaClickNum/mediaImproNum as medisClinkRid , " +
    * "adConsumption , " +
    * "adCost from tmp where mediaImproNum != 0 and bidNum != 0"
    **/
  lazy val ad_regionSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("provincename", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("cityname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("adOriginalRequestNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("avildRequestNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequestNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidSusNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidSusRid", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adImproNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaImproNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("medisClinkRid", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adConsumption", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adCost", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  lazy val ad_AppSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("appid", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("appname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidSusNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adImproNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adImproClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adConosum", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adCost", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  lazy val ad_DeviceSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("client", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("device", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidSusNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adImproNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adImproClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adConosum", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adCost", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  lazy val ad_NetworkSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("networkmannerid", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("networkmannername", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidSusNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adImproNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adImproClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adConosum", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adCost", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }


  lazy val ad_IspSchema: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("ispname", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("originalRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("validRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adRequest", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidSusNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("bidRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adImproNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("adImproClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaClickNum", Type.INT64).nullable(false).build(),
      new ColumnSchemaBuilder("mediaRit", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adConosum", Type.DOUBLE).nullable(false).build(),
      new ColumnSchemaBuilder("adCost", Type.DOUBLE).nullable(false).build()
    ).asJava
    new Schema(columns)
  }

  lazy val trade: Schema = {
    val columns = List(
      new ColumnSchemaBuilder("geoHashCode", Type.STRING).nullable(false).key(true).build(),
      new ColumnSchemaBuilder("location", Type.STRING).nullable(false).build()
    ).asJava
    new Schema(columns)
  }
}
