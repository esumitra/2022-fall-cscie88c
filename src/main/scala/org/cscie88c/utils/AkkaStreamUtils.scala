package org.cscie88c.utils

import akka.stream._
import akka.stream.scaladsl._
import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import scala.concurrent._
import scala.concurrent.duration._
import com.typesafe.config.Config

object AkkaStreamUtils {
  def actorSystemInstance(
      name: String
    ): (ActorSystem, ActorMaterializer, ExecutionContext) = {
    val akkaSystem = ActorSystem(s"$name-Actor-System")
    val akkaMaterializer = ActorMaterializer()(akkaSystem)
    val akkaStreamsEC = akkaSystem.dispatcher
    (akkaSystem, akkaMaterializer, akkaStreamsEC)
  }

  object defaultActorSystem {

    implicit val (
      akkaStreams1System,
      akkaStreams1Materializer,
      akkaStreams1EC
    ) = actorSystemInstance("Default-Actor-System")

    def shutdown() =
      akkaStreams1System.terminate()
  }

}
