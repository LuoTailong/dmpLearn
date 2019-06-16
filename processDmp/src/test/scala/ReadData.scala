import java.beans.Transient

import com.itheima.dmp.tools.GlobalConfigUtils
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu._
/**
  * Created by angel
  */
object ReadData {
  val kuduMaster = GlobalConfigUtils.kuduMaster
  val sourceTable  = "REGION"
  val options:Map[String , String] = Map(
    "kudu.master" -> kuduMaster ,
    "kudu.table" -> sourceTable
  )
  def main(args: Array[String]): Unit = {
    @Transient
    val sparkConf = new SparkConf().setAppName("APP")
      .setMaster("local[6]")
      .set("spark.worker.timeout" , GlobalConfigUtils.workerTimeout)
      .set("spark.rpc.askTimeout" , GlobalConfigUtils.rpcTimeout)
      .set("spark.cores.max" , GlobalConfigUtils.maxCores)
      .set("spark.task.maxFailures" , GlobalConfigUtils.maxFailures)
      .set("spark.speculation" , GlobalConfigUtils.speculation)
      .set("spark.driver.allowMutilpleContext" , GlobalConfigUtils.allMutilContext)
      .set("spark.serializer" , GlobalConfigUtils.serializer)
      .set("spark.buffer.pageSize" , GlobalConfigUtils.pageSize)

    val sparkContext = new SparkContext(sparkConf)
    val sQLContext = SparkSession.builder().config(sparkConf).getOrCreate().sqlContext
    val kuduContext = new KuduContext(kuduMaster , sQLContext.sparkContext)

    val kudu = sQLContext.read.options(options).kudu
    kudu.show(5)
  }
}
