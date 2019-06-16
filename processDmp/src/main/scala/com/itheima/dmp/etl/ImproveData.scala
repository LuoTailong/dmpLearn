package com.itheima.dmp.etl

import com.itheima.dmp.`trait`.DataProcess
import com.itheima.dmp.bean.la_lo_pro_city
import com.itheima.dmp.contants.{ContantsSQL, ContantsSchema}
import com.itheima.dmp.tools.{DBUtils, GlobalConfigUtils, Parse2Bean, TimeUtils}
import org.apache.kudu.Schema
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object ImproveData extends DataProcess {
  /**
    * 逻辑：
    * 1 获取数据
    * 2 拿到IP
    * 3 基于IP解析经度和纬度
    * 4 merge
    * 5 数据落地到kudu
    *   5.1 kudu的schema
    *   5.2 kudu的分区ID
    */
  val data: String = GlobalConfigUtils.getData
  //ods+时间 -->ods
  val sinkTable = GlobalConfigUtils.ods + TimeUtils.getTime()
  private val kuduMaster: String = GlobalConfigUtils.kuduMaster

  override def process(sparkContext: SparkContext, sQLContext: SQLContext, kuduContext: KuduContext): Unit = {
    //1 获取
    val loadData = sQLContext.read.format("json").load(data)
    //2 获取IP
    val rdd = loadData.rdd
    val ips: RDD[String] = rdd.map {
      line =>
        val ipData: String = line.getAs[String]("ip")
        ipData
    }
    val ipList: List[String] = ips.collect().toList
    //期望(返回一张表 IP longtitude latitude province city)
    //parse2Bean(ipList):Seq[la_lo_pro_city]
    val parse: Seq[la_lo_pro_city] = Parse2Bean.parse(ipList)
    val rddData: RDD[la_lo_pro_city] = sparkContext.parallelize(parse)
    import sQLContext.implicits._
    val df: DataFrame = rddData.toDF()
    //创建临时表
    df.createOrReplaceTempView("bean")
    loadData.createOrReplaceTempView("ods")
    //merge
    //(ip 经纬度 地域 ip)
    //select A.xxx, B.xxx from B left join A where A.ip = B.ip
    val result: DataFrame = sQLContext.sql(ContantsSQL.odssql)

    //数据落地到kudu:KUDUmaster sinkTable result schema partitionID kuduContext
    //1.1 当前插入的是哪张表
    //1.2 当前插入的是谁的result
    //1.3 数据类型是什么：schema
    //1.4 分区的ID是什么
    //1.5 这个要插入的表是否存在 如果不存在则创建 如果存在则插入

    val schema: Schema = ContantsSchema.odsSchema
    val partitionID: String = "ip"
    //result , sinkTable , kuduMaster , kuduContext , schema , partitionID
    DBUtils.write(result, sinkTable, kuduMaster, kuduContext, schema, partitionID)
  }
}
