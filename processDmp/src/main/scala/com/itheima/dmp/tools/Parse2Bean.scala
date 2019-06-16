package com.itheima.dmp.tools

import java.util
import com.itheima.dmp.bean.la_lo_pro_city
import com.itheima.dmp.tools.ips.iplocation.IPLocation
import com.maxmind.geoip.{Location, LookupService}
import com.itheima.dmp.tools.ips.iplocation.IPAddressUtils
import scala.collection.JavaConverters

object Parse2Bean {
  val GeoLiteCity: String = GlobalConfigUtils.GeoLiteCity

  def parse(ipList: List[String]): Seq[la_lo_pro_city] = {
    val array = new util.ArrayList[la_lo_pro_city]
    val lookupService = new LookupService(GeoLiteCity, LookupService.GEOIP_MEMORY_CACHE)
    if (ipList.size > 0) {
      for (ip <- ipList) {
        //解析经纬度
        val location: Location = lookupService.getLocation(ip)
        //经度
        val longitude: Float = location.longitude
        //纬度
        val latitude: Float = location.latitude
        //解析省份和地域信息
        val iPAddressUtils: IPAddressUtils = new IPAddressUtils
        val ipLocation: IPLocation = iPAddressUtils.getregion(ip)
        //省份
        val province: String = ipLocation.getRegion
        //城市
        val city: String = ipLocation.getCity
        array.add(la_lo_pro_city(ip,longitude.toString,latitude.toString,province,city))
      }
    }
    val result: Seq[la_lo_pro_city] = JavaConverters.asScalaIteratorConverter(array.iterator()).asScala.toSeq
    result
  }

}
