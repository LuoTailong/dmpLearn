package com.itheima.dmp

import java.beans.Transient

import com.itheima.dmp.etl._
import com.itheima.dmp.tools.GlobalConfigUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object APP {
  val kuduMaster: String = GlobalConfigUtils.kuduMaster

  def main(args: Array[String]): Unit = {
    @Transient
    val sparkConf = new SparkConf().setAppName("APP")
      .setMaster("local[6]")
      .set("spark.worker.timeout", GlobalConfigUtils.workerTimeout)
      .set("spark.rpc.askTimeout", GlobalConfigUtils.rpcTimeout)
      .set("spark.cores.max", GlobalConfigUtils.maxCores)
      .set("spark.task.maxFailures", GlobalConfigUtils.maxFailures)
      .set("spark.speculation", GlobalConfigUtils.speculation)
      .set("spark.driver.allowMutilpleContext", GlobalConfigUtils.allMutilContext)
      .set("spark.serializer", GlobalConfigUtils.serializer)
      .set("spark.buffer.pageSize", GlobalConfigUtils.pageSize)

    val sparkContext = new SparkContext(sparkConf)
    val sQLContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    val kuduContext = new KuduContext(kuduMaster, sQLContext.sparkContext)

    //写业务
    //TODO 1 基于IP解析出经纬度和地域信息
    //ImproveData.process(sparkContext, sQLContext, kuduContext)

    //TODO 2 查看地域的分布情况
    //select province ,city, count(1) from ods group by province,city
    //RegionAnalysis.process(sparkContext, sQLContext, kuduContext)

    //TODO 3 广告投放的地域分布情况
    //AdRegion.process(sparkContext, sQLContext, kuduContext)

    //TODO 4 广告投放的APP分布情况
    //APPAnalysis.process(sparkContext,sQLContext,kuduContext)

    //TODO 5 手机设备
    Device.process(sparkContext , sQLContext , kuduContext)
    //TODO 6 广告投放的network的分布情况

    //TODO 7 广告投放的网络运营商的分布情况

    //TODO 商圈库

    //TODO 打标签


    if (!sparkContext.isStopped) {
      sparkContext.stop()
    }
  }
}
