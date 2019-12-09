package com.flink.hotItems


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 先定义输入数据的样例类
case class UserBehavior(userId:Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long)
// 定义一个窗口聚合结构样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItems {

  def main(args: Array[String]): Unit = {
    // 1. create environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. read data
    val path: String = this.getClass.getResource("UserBehavior.csv").getPath
    val dataStream: DataStream[UserBehavior] = env.readTextFile(path)
      .map(data=>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim,dataArray(4).trim.toLong)
      })// 文本数据已经按照时间排好序
      .assignAscendingTimestamps(_.timestamp * 1000L) // 转化为ms

    //3. transformation
    val processed = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5)) // 窗口大小为 1h，步长为 5min
      .aggregate(new CountAgg(),new WindowResult())   // 窗口聚合
      .keyBy(_.windowEnd)  // 按照窗口分组
        .process(new TopNHotItems())
    //4. sink
    dataStream.print()

    env.execute("hot items job")

  }
}

// 自定义预聚合函数
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{ // 第二个参数为累加器类型
  // 累加器初值
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，输出ItermViewCount
class WindowResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

// 自定义的处理函数
class TopNHotItems(topSize:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
  private var itemState:ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit ={
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state",classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 把每条数据存入状态列表
  }

}