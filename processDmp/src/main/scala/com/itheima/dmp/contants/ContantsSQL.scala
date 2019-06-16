package com.itheima.dmp.contants


object ContantsSQL {

  //1:merge
  //1：初始化，将经纬度和地域merge到ods中
  lazy val odssql = "select " +
    "ods.ip ," +
    "ods.sessionid," +
    "ods.advertisersid," +
    "ods.adorderid," +
    "ods.adcreativeid," +
    "ods.adplatformproviderid" +
    ",ods.sdkversion" +
    ",ods.adplatformkey" +
    ",ods.putinmodeltype" +
    ",ods.requestmode" +
    ",ods.adprice" +
    ",ods.adppprice" +
    ",ods.requestdate" +
    ",ods.appid" +
    ",ods.appname" +
    ",ods.uuid, ods.device, ods.client, ods.osversion, ods.density, ods.pw, ods.ph" +
    ",bean.longitude as long" +
    ",bean.latitude as lat" +
    ",bean.province as provincename" +
    ",bean.city as cityname" +
    ",ods.ispid, ods.ispname" +
    ",ods.networkmannerid, ods.networkmannername, ods.iseffective, ods.isbilling" +
    ",ods.adspacetype, ods.adspacetypename, ods.devicetype, ods.processnode, ods.apptype" +
    ",ods.district, ods.paymode, ods.isbid, ods.bidprice, ods.winprice, ods.iswin, ods.cur" +
    ",ods.rate, ods.cnywinprice, ods.imei, ods.mac, ods.idfa, ods.openudid,ods.androidid" +
    ",ods.rtbprovince,ods.rtbcity,ods.rtbdistrict,ods.rtbstreet,ods.storeurl,ods.realip" +
    ",ods.isqualityapp,ods.bidfloor,ods.aw,ods.ah,ods.imeimd5,ods.macmd5,ods.idfamd5" +
    ",ods.openudidmd5,ods.androididmd5,ods.imeisha1,ods.macsha1,ods.idfasha1,ods.openudidsha1" +
    ",ods.androididsha1,ods.uuidunknow,ods.userid,ods.iptype,ods.initbidprice,ods.adpayment" +
    ",ods.agentrate,ods.lomarkrate,ods.adxrate,ods.title,ods.keywords,ods.tagid,ods.callbackdate" +
    ",ods.channelid,ods.mediatype,ods.email,ods.tel,ods.sex,ods.age from ods left join " +
    "bean on ods.ip=bean.ip where ods.ip is not null"

  //受众目标地域分布
  lazy val regionAnalysisSQL = "select provincename , cityname , count(1) as count from ods group by provincename , cityname"

  //3：广告投放的地域分布
  lazy val adRegionTmp = "select provincename , cityname , " +
    "sum(case when requestmode=1 and processnode >= 1 then 1 else 0 end) adOriginalRequestNum , " +
    "sum(case when requestmode=1 and processnode >= 2 then 1 else 0 end) avildRequestNum , " +
    "sum(case when requestmode=1 and processnode = 3 then 1 else 0 end) adRequestNum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid != 0 then 1 else 0 end) bidNum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) bidSusNum , " +
    "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) adImproNum , " +
    "sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) adClickNum , " +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) mediaImproNum , " +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) mediaClickNum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid >200000 and adcreativeid > 200000 then 1*winprice/1000 else 0 end) adConsumption , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid >200000 and adcreativeid > 200000 then 1*adpayment/1000 else 0 end) adCost " +
    "from ods group by provincename , cityname"
  //做除法操作
  lazy val adRegion = "select provincename , cityname , " +
    "adOriginalRequestNum , " +
    "avildRequestNum , " +
    "adRequestNum , " +
    "bidNum , " +
    "bidSusNum , " +
    "bidSusNum/bidNum as bidSusRid , " +
    "adImproNum , " +
    "adClickNum , " +
    "mediaImproNum , " +
    "mediaClickNum , " +
    "mediaClickNum/mediaImproNum as medisClinkRid , " +
    "adConsumption , " +
    "adCost from tmp where mediaImproNum != 0 and bidNum != 0"

  //4 求广告投放的APP分布情况
  lazy val adApptmp = "select appid,appname," +
    "sum(case when requestmode<=2 and processnode=1 then 1 else 0 end) originalRequest ," +
    "sum(case when requestmode>=1 and processnode=2 then 1 else 0 end) validRequest ," +
    "sum(case when requestmode=1 and processnode=3 then 1 else 0 end) adRequest ," +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid != 0 then 1 else 0 end) bidNum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid != 0 then 1 else 0 end) bidSusNum , " +
    "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) adImproNum , " +
    "sum(case when requestmode=3 and iseffective=1 and adorderid != 0 then 1 else 0 end) adImproClickNum , " +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end) mediaNum , " +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end) mediaClickNum , +" +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid > 200000 and adcreativeid > 200000 then 1*winprice/1000 else 0 end) adConosum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 and adorderid > 200000 and adcreativeid > 200000 then 1*adpayment/1000 else 0 end) adCost from tmp group by appid , appname"

  lazy val adApp = "select appid , " +
    "appname , " +
    "originalRequest , " +
    "validRequest , " +
    "adRequest , " +
    "bidNum , " +
    "bidSusNum , " +
    "bidSusNum/bidNum bidRit , " +
    "adImproNum , " +
    "adImproClickNum , " +
    "mediaNum , " +
    "mediaClickNum , " +
    "mediaClickNum/mediaNum mediaRit , " +
    "adConosum , " +
    "adCost from app_analysis where bidNum != 0 and mediaNum != 0"

  //5:求广告投放的Device分布情况
  lazy val adDeviceTmp = "select client , device , " +
    "sum(case when  requestmode<=2 and processnode=1  then 1 else 0 end) originalRequest , " +
    "sum(case when  requestmode>=1 and processnode>=2  then 1 else 0 end) validRequest , " +
    "sum(case when requestmode=1 and processnode=3 then 1 else 0 end) adRequest , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid != 0 then 1 else 0 end) bidNum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid != 0 then 1 else 0 end) bidSusNum , " +
    "sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) adImproNum , " +
    "sum(case when requestmode=3 and iseffective=1 and adorderid != 0 then 1 else 0 end) adImproClickNum , " +
    "sum(case when requestmode=2 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end) mediaNum , " +
    "sum(case when requestmode=3 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 then 1 else 0 end) mediaClickNum , +" +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid > 200000 and adcreativeid > 200000 then 1*winprice/1000 else 0 end) adConosum , " +
    "sum(case when adplatformproviderid >= 100000 and iseffective=1 and isbilling=1 and isbid=1 and iswin=1 and adorderid > 200000 and adcreativeid > 200000 then 1*adpayment/1000 else 0 end) adCost " +
    "from tmp group by client , device"

  lazy val adDevice = "select client , " +
    "device , " +
    "originalRequest , " +
    "validRequest , " +
    "adRequest , " +
    "bidNum , " +
    "bidSusNum , " +
    "bidSusNum/bidNum bidRit , " +
    "adImproNum , " +
    "adImproClickNum , " +
    "mediaNum , " +
    "mediaClickNum , " +
    "mediaClickNum/mediaNum mediaRit , " +
    "adConosum , " +
    "adCost from device_analysis where bidNum != 0 and mediaNum != 0"
}


