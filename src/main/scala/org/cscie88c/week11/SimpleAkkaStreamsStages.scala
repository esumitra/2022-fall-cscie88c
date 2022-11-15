package org.cscie88c.week11

import akka.stream._
import akka.stream.scaladsl._
import akka.{ Done, NotUsed }
import akka.util.ByteString
import akka.actor.ActorSystem
import scala.concurrent._
import scala.concurrent.duration._
import org.cscie88c.utils.{AkkaStreamUtils}
import cats._
import cats.implicits._
import java.nio.file.Paths
import scala.concurrent.{ Future }
import scala.util.{ Failure, Success, Try }
import scala.concurrent.duration._

object SimpleAkkaStreamsStages {
  
  /**
    * This is a simple example of a flow that reads a file and returns the lines as a stream of strings.
    *
    * @param path path to the file to read
    * @return
    */
  def fileInputSource(path: String): Source[String, Future[IOResult]] = {
    val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(Paths.get(path))
    val lineSource: Source[ByteString,Future[IOResult]]  = source.via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 256, allowTruncation = true))
    lineSource.map(_.utf8String)
  }

  /**
   * This is a simple flow that transforms a type to a ByteString to write to a file
   * */
  def byteStringFlow[A](): Flow[A, ByteString, NotUsed] = {
    Flow[A].map(a => ByteString(s"$a\n"))
  }

  /**
    * This is a simple Sink that writes to a file
    *
    * @param path
    * @return
    */
  def fileOutputSink[A](path: String): Sink[A, Future[IOResult]] = {
    val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(Paths.get(path))
    byteStringFlow().toMat(sink)(Keep.right)
  }

  /**
   * calculates aagregate in batches of groupSize and 
   */
  def groupedAggregate[A: Monoid](groupSize: Int): Flow[A,A,NotUsed] = {
    Flow[A].grouped(groupSize).map { group =>
      group.reduce(_ |+| _)
    }
  }

  /**
   * This is a simple flow that filters None elements
   *
   * @return
   */
  def someFlow[A](): Flow[Option[A], A, NotUsed] = {
    Flow[Option[A]].collect {
      case Some(a) => a
    }
  }

  val transactionAggregateFlow: Flow[CustomerTransaction, AverageTransactionAggregate, NotUsed] =
    Flow[CustomerTransaction].map(AverageTransactionAggregate.apply)

  val averageMetricFlow: Flow[AverageTransactionAggregate, Double, NotUsed] =
    Flow[AverageTransactionAggregate].map(_.averageAmount)  
}
