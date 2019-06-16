package com.itheima.dmp.etl

import com.itheima.dmp.`trait`.DataProcess
import com.itheima.dmp.contants.{ContantsSQL, ContantsSchema}
import com.itheima.dmp.tools.{DBUtils, GlobalConfigUtils, TimeUtils}
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.kudu.spark.kudu._

object APPAnalysis extends DataProcess {
  /**
    * 1 拿到当天ods数据集
    * 2 报表操作 2步
    * 3 数据落地操作
    */
  val kuduMaster: String = GlobalConfigUtils.kuduMaster
  val sourceTable = GlobalConfigUtils.ods + TimeUtils.getTime()
  val sinkTable = GlobalConfigUtils.getAdApp
  private val options: Map[String, String] = Map(
    "kudu.master" -> kuduMaster,
    "kudu.table" -> sourceTable
  )

  override def process(sparkContext: SparkContext, sQLContext: SQLContext, kuduContext: KuduContext): Unit = {
    //1 拿到当天的数据集
    val source: DataFrame = sQLContext.read.options(options).kudu

    //2 报表操作
    source.createOrReplaceTempView("tmp")
    val tmpData: DataFrame = sQLContext.sql(ContantsSQL.adApptmp)
    //最终临时表名
    tmpData.createOrReplaceTempView("app_analysis")
    val result: DataFrame = sQLContext.sql(ContantsSQL.adApp)
    val schema: Schema = ContantsSchema.ad_AppSchema
    val partitionID = "appid"
    //result , sinkTable , kuduMaster , kuduContext , schema , partitionID
    DBUtils.write(result, sinkTable, kuduMaster, kuduContext, schema, partitionID)
  }
}
