package org.cscie88c.week11

import cats._
import cats.implicits._

final case class AverageTransactionAggregate(
    customerId: String,
    totalAmount: Double,
    count: Long
  ) {
  def averageAmount: Double = totalAmount / count
}

object AverageTransactionAggregate {
  def apply(raw: CustomerTransaction): AverageTransactionAggregate =
    AverageTransactionAggregate(raw.customerId, raw.transactionAmount, 1L)

  implicit val averageTransactionMonoid: Monoid[AverageTransactionAggregate] =
    new Monoid[AverageTransactionAggregate] {
      override def empty: AverageTransactionAggregate =
        AverageTransactionAggregate("", 0.0, 0L)

      override def combine(
          x: AverageTransactionAggregate,
          y: AverageTransactionAggregate
        ): AverageTransactionAggregate =
        AverageTransactionAggregate(
          x.customerId,
          x.totalAmount + y.totalAmount,
          x.count + y.count
        )
    }
}
