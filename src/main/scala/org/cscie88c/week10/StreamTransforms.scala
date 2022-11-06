package org.cscie88c.week10

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }
import org.apache.kafka.streams.scala.{ Serdes, StreamsBuilder }
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable }
import org.apache.kafka.streams.kstream.{ Suppressed, TimeWindows, Windowed }
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import com.goyeau.kafka.streams.circe.CirceSerdes._
import io.circe.generic.auto._
import java.time.Duration
import cats._
import cats.implicits._
import io.circe.parser.decode
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe
import org.apache.kafka.streams.kstream.SessionWindows
import org.apache.kafka.streams.kstream.JoinWindows
object StreamTransforms {

  // automatic encoding decoding of values
  import Serdes._

  /** a stateful computation of word counts
    *
    * @param inputTopic
    * @param outputTopic
    * @param storeName
    * @return
    */
  def wordCount(
      inputTopic: String,
      outputTopic: String,
      storeName: String
    ): Topology = {
    val builder: StreamsBuilder = new StreamsBuilder
    val source: KStream[String, String] =
      builder.stream[String, String](inputTopic)
    val transform: KTable[String, Long] = source
      .flatMapValues(textLine => textLine.toLowerCase().split("\\W+"))
      .groupBy((_, word) => word)
      .count()(Materialized.as(storeName))
    transform.toStream.to(outputTopic)
    builder.build()
  }




  /** calculate a running total of transaction amounts per account per minute using a sliding window
    *
    * @param inputTopic
    * @param outputTopic
    * @param storeName
    * @return
    */
  def transactionTotalsPerAccountPerMinute(
      inputTopic: String,
      outputTopic: String,
      storeName: String
    ): Topology = {
    val builder = new StreamsBuilder
    val source: KStream[String, Transaction] =
      builder.stream[String, Transaction](inputTopic)
    val minuteWindow =
      TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofSeconds(0))
    val transform: KTable[Windowed[String], Double] = source
      .map((_, t) =>
        (t.accountNumber, t.amount)
      ) // 1. key by account number, map value to transaction amount
      .groupByKey // 2. group by account number
      .windowedBy(minuteWindow) // 3. window by every minute
      .reduce(_ + _)(Materialized.as(storeName)) // 4. sum amounts
      .suppress(
        Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
      ) // only one sum per window
    transform.toStream((k, v) => (s"${k.key()} -- ${k.window()}", v.toString())).to(outputTopic)
    builder.build()
  }

}
