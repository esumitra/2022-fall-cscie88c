package org.cscie88c.week11

import scala.util.{ Failure, Success, Try }

final case class CustomerTransaction(
    customerId: String,
    transactionDate: String,
    transactionAmount: Double
  ) {
  def transactionYear: String =
    transactionDate.split("-")(2)
  def transactionCategory: String =
    if (transactionAmount > 80) "High"
    else "Standard"
}

object CustomerTransaction {
  def apply(csvRow: String): Option[CustomerTransaction] = Try {
    val fields = csvRow.split(",")
    CustomerTransaction(
      customerId = fields(0),
      transactionDate = fields(1),
      transactionAmount = fields(2).toDouble
    )
  }.toOption

}
