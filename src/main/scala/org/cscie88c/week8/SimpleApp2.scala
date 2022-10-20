package org.cscie88c.week8

import com.typesafe.scalalogging.LazyLogging
import scala.util.{Try, Success, Failure}
import scala.io.Source
import org.cscie88c.config.{ConfigUtils}
import pureconfig.generic.auto._

// write case class here

// run with: sbt "runMain org.cscie88c.week8.SimpleApp2"
object SimpleApp2 extends LazyLogging{
  

  def lineStreamFromFile(fileName: String): Option[LazyList[String]] =
    Try {
      LazyList.from(Source.fromResource(fileName).getLines())
    }.toOption
  
  def monthLines(month: String)(lines: LazyList[String]): LazyList[String] = ???
      
  def main(args: Array[String]): Unit = ???
}
