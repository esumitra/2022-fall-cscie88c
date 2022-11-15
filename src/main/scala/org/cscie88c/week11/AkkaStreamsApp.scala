package org.cscie88c.week11

import scala.util.{Try, Success, Failure}
import com.typesafe.scalalogging.{LazyLogging}
import org.cscie88c.config.{ConfigUtils}
import pureconfig.generic.auto._
import akka.stream._
import akka.stream.scaladsl._
import akka.{ Done, NotUsed }
import scala.concurrent.Future
import org.cscie88c.utils.{AkkaStreamUtils}

case class AkkaStreamsAppConfig(
  name: String,
  inputTransactionFile: String,
  outputTransactionFile: String,
)

// run with: sbt "runMain org.cscie88c.week11.AkkaStreamsApp"
object AkkaStreamsApp extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("[XXXX]: Start of AkkaStreamsApp")
    val appSettings = ConfigUtils.loadAppConfig[AkkaStreamsAppConfig]("org.cscie88c.simple-akkastreams-app")
    logger.info(s"[settings]: $appSettings")
    transformData(appSettings)
    logger.info("[XXXX]: Stopped AkkaStreamsApp")
  }

  def transformData(conf: AkkaStreamsAppConfig): Unit = {
    import AkkaStreamUtils.defaultActorSystem._

    val pipeline: RunnableGraph[Future[IOResult]] = simplePipeline(conf.inputTransactionFile, conf.outputTransactionFile)

    pipeline.run().onComplete {
      case Success(IOResult(_,_)) => {
        logger.info("[XXXX]: Done")
        shutdown()
      }
      case Failure(ex) => {
        logger.error(s"[XXXX]: Failed: ${ex.getMessage}")
        shutdown()
      }
    }
  }

  /**
    * data processing pipeline built by assembling reusables stages
    * writes the average transaction value, for each group of 1000 transactions
    * @param inPath
    * @param outPath
    * @return
    */
  def simplePipeline(inPath: String, outPath: String): RunnableGraph[Future[IOResult]] = {
    import SimpleAkkaStreamsStages._
    fileInputSource(inPath)
      .drop(1)
      .via(Flow[String].map(line => CustomerTransaction(line)))
      .via(someFlow())
      .via(transactionAggregateFlow)
      .via(groupedAggregate(1000))
      .via(averageMetricFlow)
      .toMat(fileOutputSink(outPath))(Keep.right)
  }
}
