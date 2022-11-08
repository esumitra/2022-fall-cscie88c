package org.cscie88c.week10

import java.time.LocalDateTime

final case class Transaction(
    number: Long,
    cardType: String,
    amount: Double,
    accountNumber: String,
    expiry: LocalDateTime,
    transactionTime: LocalDateTime
)
