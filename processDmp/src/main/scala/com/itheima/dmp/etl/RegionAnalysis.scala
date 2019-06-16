package com.itheima.dmp.etl

import com.itheima.dmp.`trait`.DataProcess
import com.itheima.dmp.contants.{ContantsSQL, ContantsSchema}
import com.itheima.dmp.tools.{DBUtils, GlobalConfigUtils, TimeUtils}
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.kudu.spark.kudu._

/**
  * 1: 获取的当日ODS的数据集
  * 2：基于获取的数据集进行报表
  * 3：数据落地KUDU
  */
object RegionAnalysis extends DataProcess{
  val kuduMaster: String = GlobalConfigUtils.kuduMaster
  val sourceTable = GlobalConfigUtils.ods+TimeUtils.getTime()
  val sinkTable: String = GlobalConfigUtils.getRegion
  val options:Map[String , String] = Map(
    "kudu.master" -> kuduMaster ,
    "kudu.table" -> sourceTable
  )
  override def process(sparkContext: SparkContext, sQLContext: SQLContext, kuduContext: KuduContext): Unit = {
    //1 获取当日的ODS的数据集
    val sourceData: DataFrame = sQLContext.read.options(options).kudu
    //2 基于获取的数据集进行报表
    //需求：求各省份和城市的地域分布情况
    //实现：select province, city , count(1) from ods20190106 group by province , city
    sourceData.createOrReplaceTempView("ods")
    val result:DataFrame = sQLContext.sql(ContantsSQL.regionAnalysisSQL)

    //3 数据落地KUDU
    val schema: Schema = ContantsSchema.regionSchema
    val partitionID = "provincename"
    DBUtils.write(result,sinkTable,kuduMaster,kuduContext,schema,partitionID)
  }
}
