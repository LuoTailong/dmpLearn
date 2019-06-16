package com.itheima.dmp.etl

import com.itheima.dmp.`trait`.DataProcess
import com.itheima.dmp.contants.{ContantsSQL, ContantsSchema}
import com.itheima.dmp.tools.{DBUtils, GlobalConfigUtils, TimeUtils}
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.kudu.spark.kudu._

object Device extends DataProcess {
  val kuduMaster = GlobalConfigUtils.kuduMaster
  val sourceTable = GlobalConfigUtils.ods + TimeUtils.getTime()
  val sinkTable = GlobalConfigUtils.getDevice
  private val options: Map[String, String] = Map(
    "kudu.Master" -> kuduMaster,
    "kudu.table" -> sourceTable
  )

  override def process(sparkContext: SparkContext, sQLContext: SQLContext, kuduContext: KuduContext): Unit = {
    //1 加载ods数据集
    val source: DataFrame = sQLContext.read.options(options).kudu
    source.createOrReplaceTempView("tmp")
    val tmp: DataFrame = sQLContext.sql(ContantsSQL.adDeviceTmp)
    tmp.createOrReplaceTempView("device_analysis")
    val result: DataFrame = sQLContext.sql(ContantsSQL.adDevice)
    val schema: Schema = ContantsSchema.ad_DeviceSchema
    val partitionID = "client"
    DBUtils.write(result, sinkTable, kuduMaster, kuduContext, schema, partitionID)
  }
}
