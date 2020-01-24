package example

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID

import better.files.Dsl._
import better.files.File
import better.files.File._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

object Generator extends App {
  val productsNumber = 500000
  val storesNumber = 3000
  val txNumber = 1000000
  val daysNumber = 6
  val maxQty = 10

  val outputDir = (currentWorkingDirectory / "generated").createDirectoryIfNotExists()
  val outputDirPath = outputDir.pathAsString
  val random = Random

  val storeUuids = (1 to storesNumber).map(_ => UUID.randomUUID().toString)
  val prices = Array("1.23", "4.54", "2.41")

  // generate data for each days
  (0 to daysNumber).foreach(day => {
    val dateKey = LocalDate.now().minus(day, ChronoUnit.DAYS).format(DateTimeFormatter.BASIC_ISO_DATE)
    println(s"START $dateKey")
    (generateTx(dateKey) :: generateStoreRef(dateKey)).foreach(fu => Await.result(fu, Duration.Inf))
    println(s"END $dateKey")
  })

  private def generateTx(dateKey: String) = {
    val txFile = File(s"$outputDirPath/transactions_$dateKey.data").createIfNotExists()
   Future {
      (1 to txNumber).map(i => {
        val storeUuid = storeUuids(random.nextInt(storesNumber))
        val productId = random.nextInt(productsNumber) + 1
        val qty = random.nextInt(maxQty) + 1
        s"$i|$dateKey|$storeUuid|$productId|$qty"
      }).mkString("\n") >>: txFile
    }
  }

  private def generateStoreRef(dateKey: String) = {
    storeUuids.map(uuid => {
      Future {
        val file = File(s"$outputDirPath/reference_prod-${uuid}_$dateKey.data").createIfNotExists()
        (1 to productsNumber).map(i => {
          val price = prices(i % prices.length)
          s"$i|$price"
        }).mkString("\n") >>: file
      }
    }).toList
  }
}
