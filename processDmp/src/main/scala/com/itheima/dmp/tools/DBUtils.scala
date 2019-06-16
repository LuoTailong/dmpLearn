package com.itheima.dmp.tools

import java.util
import org.apache.kudu.Schema
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.kudu.spark.kudu._

object DBUtils {
  //result sinkTable kuduMaster kuduContext schema partitionID
  def write(result: DataFrame
            , sinkTable: String
            , kuduMaster: String
            , kuduContext: KuduContext
            , schema: Schema
            , partitionID: String) = {
    //1 如果表不存在则创建
    if (!kuduContext.tableExists(sinkTable)) {
      val kuduClientBuilder = new KuduClientBuilder(kuduMaster).build()
      //1.1 指定表的分区方式
      val tableOptions: CreateTableOptions = {
        val list = new util.LinkedList[String]()
        list.add(partitionID)

        new CreateTableOptions().addHashPartitions(list, 6).setNumReplicas(3)
      }
        kuduClientBuilder.createTable(sinkTable, schema, tableOptions)
    }

    //2 如果表存在则插入
    result.write.mode(SaveMode.Append).option("kudu.table", sinkTable).option("kudu.master", kuduMaster).kudu
  }
}
