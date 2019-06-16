package com.itheima.dmp.`trait`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

trait DataProcess {
  def process(sparkContext: SparkContext,sQLContext: SQLContext,kuduContext: KuduContext)
}
