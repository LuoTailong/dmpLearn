package com.itheima.dmp.tools

import com.typesafe.config.ConfigFactory

class GlobalConfigUtils {
  def conf = ConfigFactory.load()

  //TODO 获取spark的配置参数
  def workerTimeout = conf.getString("spark.worker.timeout")

  def rpcTimeout = conf.getString("spark.rpc.askTimeout")

  def maxCores = conf.getString("spark.cores.max")

  def maxFailures = conf.getString("spark.task.maxFailures")

  def speculation = conf.getString("spark.speculation")

  def allMutilContext = conf.getString("spark.driver.allowMutilpleContext")

  def serializer = conf.getString("spark.serializer")

  def pageSize = conf.getString("spark.buffer.pageSize")

  //master
  def kuduMaster = conf.getString("kudu.master")

  //path.data
  def getData = conf.getString("data.path")

  //GeoLiteCity
  def GeoLiteCity = conf.getString("GeoLiteCity")

  //qqwry

  def IP_FILE = conf.getString("IP_FILE")

  def INSTALL_DIR = conf.getString("INSTALL_DIR")

  def ods = conf.getString("DB.ODS")

  def getRegion = conf.getString("DB.REGION")

  def getAdRegion = conf.getString("DB.AD.REGION")

  def getAdApp = conf.getString("DB.AD.APP")

  def getDevice = conf.getString("DB.AD.DEVICE")
}

object GlobalConfigUtils extends GlobalConfigUtils {
  def main(args: Array[String]): Unit = {
    print(IP_FILE, INSTALL_DIR)
  }
}
