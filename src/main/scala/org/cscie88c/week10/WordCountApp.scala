package org.cscie88c.week10

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig }
import java.nio.file.Paths

// run with: sbt "runMain org.cscie88c.week10.WordCountApp"
object WordCountApp {
  def fullPath(dir: String): String = Paths.get(dir).normalize().toAbsolutePath().toString()

  def main(args: Array[String]): Unit = {
    import Serdes._
    val statePath = fullPath("./kafkastate")
    // 1. define kafka streams properties, usually from a config file
    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-application-client")
      p.put(StreamsConfig.STATE_DIR_CONFIG, statePath)
      p
    }

    // 2. create KStreams DSL instance
    val builder: StreamsBuilder = new StreamsBuilder
    val textLines: KStream[String, String] =
      builder.stream[String, String]("TextLinesTopic")

    // 3. transform the data
    val wordCounts: KTable[String, Long] = textLines
      .flatMapValues(textLine => textLine.toLowerCase().split("\\W+"))
      .groupBy((_, word) => word)
      .count()(Materialized.as("wordcount"))

    // 4. write the results to a topic or other persistent store
    wordCounts
      .mapValues((word, count) => s"$word: $count")
      .toStream
      // .peek((k,t) => println(s"stream element: $k: $t")) // to print items in stream
      .to("WordsWithCountsTopic")

    // 5. start the streams application
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // 6. attach shutdown handler to catch control-c
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

}
