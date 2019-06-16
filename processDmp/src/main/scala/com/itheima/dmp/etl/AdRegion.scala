package com.itheima.dmp.etl

import com.itheima.dmp.`trait`.DataProcess
import com.itheima.dmp.contants.{ContantsSQL, ContantsSchema}
import com.itheima.dmp.tools.{DBUtils, GlobalConfigUtils, TimeUtils}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.kudu.spark.kudu._

/**
  * 业务逻辑：
  * 1 获取当天的ODS数据集
  * 2 进行报表操作
  *   2.1 第一个SQL求出：省/市 原始请求数 有效请求数 广告请求数 参与竞价数
  * 竞价成功数 展示量 点击量 广告成本 广告消费
  *   2.2 第二个SQL求出：省/市 原始请求数 有效请求数 广告请求数 参与竞价数
  * 竞价成功数 竞价成功率 展示量 点击量 点击率 广告成本 广告消费
  * 3 数据落地
  */
object AdRegion extends DataProcess {
  val kuduMaster: String = GlobalConfigUtils.kuduMaster
  val sourceTable = GlobalConfigUtils.ods + TimeUtils.getTime()
  private val sinkTable: String = GlobalConfigUtils.getAdRegion
  val options: Map[String, String] = Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" -> sourceTable
  )

  override def process(sparkContext: SparkContext, sQLContext: SQLContext, kuduContext: KuduContext): Unit = {
    //1 获取当天的ODS数据集
   val odsData:DataFrame = sQLContext.read.options(options).kudu
    //2 进行报表操作
    odsData.createOrReplaceTempView("ods")
    //2.1 求第一个SQL
    val tmp: DataFrame = sQLContext.sql(ContantsSQL.adRegionTmp)
    tmp.createOrReplaceTempView("tmp")

    val result = sQLContext.sql(ContantsSQL.adRegion)

    val schema = ContantsSchema.ad_regionSchema
    val partitionID = "provincename"
    //数据落地
    //result , sinkTable , kuduMaster , kuduContext , schema , partitionID
    DBUtils.write(result , sinkTable , kuduMaster , kuduContext , schema , partitionID)
  }
}
