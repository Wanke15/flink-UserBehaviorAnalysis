package com.flink.hotItems

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

// 先定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
// 定义一个窗口聚合结构样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {

  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val path: String = this.getClass.getResource("UserBehavior.csv").getPath

    val dataStream: DataStream[UserBehavior] = env.readTextFile(path)
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })





  }
}
