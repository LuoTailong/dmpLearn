package com.itheima.dmp.graphx

import com.itheima.dmp.tag.UserIds
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer
import java.util

object UserGraph {
  def graph(vertexid: RDD[(String, (List[(String, Int)], List[(String, Double)]))], rdd: RDD[Row]) = {
    //G = (V, E, D)
    //构建点集合
    val vertex: RDD[(VertexId, (List[(String, Int)], List[(String, Double)]))] = vertexid.mapPartitions {
      var listBuffer = new ListBuffer[(VertexId, (List[(String, Int)], List[(String, Double)]))]()
      line =>
        line.foreach {
          x =>
            listBuffer.append((x._1.hashCode.toLong,x._2))
        }
        listBuffer.iterator
    }

    /*//边集合
    val edge = rdd.map {
      line =>
        util.LinkedList[String] = UserIds.getUserId(line)
        var listBuffer = new ListBuffer[String]()
        for (index <- 0 until allUserids.size()) {
          listBuffer.append(allUserids.get(index).toString)
        }
        val uid = allUserids.getFirst.hashCode.toLong
        Edge(uid, listBuffer.toString().hashCode.toLong)
    }

    //构建图
    Graph.(vertex,edge)
    graph(.connectedC)*/
  }
}
