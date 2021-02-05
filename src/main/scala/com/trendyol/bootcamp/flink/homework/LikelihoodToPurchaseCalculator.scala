package com.trendyol.bootcamp.flink.homework

import java.time.Duration

import com.trendyol.bootcamp.flink.common.{RandomEventSource, _}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

case class PurchaseLikelihood(userId: Int, productId: Int, likelihood: Double)

object LikelihoodToPurchaseCalculator {

  val l2pCoefficients = Map(
    AddToBasket         -> 0.4,
    RemoveFromBasket    -> -0.2,
    AddToFavorites      -> 0.7,
    RemoveFromFavorites -> -0.2,
    DisplayBasket       -> 0.5
  )
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    System.out.println(l2pCoefficients.size.toDouble)

    val keyedStream = env
      .addSource(new RandomEventSource)
      .filter(e => List(AddToFavorites, RemoveFromFavorites, AddToBasket, RemoveFromBasket, DisplayBasket).contains(e.eventType))
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[Event] {
            override def extractTimestamp(element: Event, recordTimestamp: Long): Long =
              element.timestamp
          }
        )
    )
      .keyBy(k=> (k.userId,k.productId))

    keyedStream
      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
      .process(new ProcessWindowFunction[Event, PurchaseLikelihood, (Int,Int) , TimeWindow] {
        override def process(
                              key: (Int, Int),
                              context: Context,
                              elements: Iterable[Event],
                              out: Collector[PurchaseLikelihood]): Unit = {
          //TODO
          //l2pCoefficients.map( x=> if(x._1==elements.map(e=>e.eventType) {x._2}
          out.collect(PurchaseLikelihood(
            key._1,
            key._2,
            l2pCoefficients
              .get(
                elements
                  .map(e => e.eventType)
                  .head)
              .getOrElse(0).asInstanceOf[Double])
          )
        }
      }
      )
      .addSink(new PrintSinkFunction[PurchaseLikelihood])

    env.execute("Event Streamer")




  }


}
