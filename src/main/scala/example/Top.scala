package example

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, OffsetDateTime}
import java.util.concurrent.Executors

import better.files.File
import better.files.File._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

// TODO Manage IO Exceptions
// TODO Manage parsing/conversion exceptions (file names pattern matching, toInt/toFloat..)
// TODO Add unit tests
object Top extends App {

  case class TxLine(store: String, product: String, qty: Int)

  case class RefLine(product: String, price: Float)

  case class Aggregate(product: String, qty: Int = 0, ca: Float = 0)

  // Emulate a 2 cores CPU Limit
  implicit val context = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

  val initialDate = LocalDate.now()
  val outputDir = (currentWorkingDirectory / "results").createDirectoryIfNotExists()
  val outputDirPath = outputDir.pathAsString
  val dataFolder = currentWorkingDirectory / "generated"
  val daysNumber = 6

  if (dataFolder.exists && dataFolder.isDirectory) {
    runProgram
  } else {
    println(s"No data files found in ${dataFolder.name}")
  }

  /**
   * Limit : 2 cores CPU; RAM 512Mo
   * This program assumes that a transaction file and 2 (= nb of CPU cores) store references files can fit into RAM.
   * 10M TxLine => 360Mo
   * 1M RefLine => 20Mo (x2)
   */
  private def runProgram = {
    val started = OffsetDateTime.now()
    println(s"Started: ${started.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")

    // Per day, store->transactions rankings
    dataFolder.children
      .foreach { file =>
        file.name match {
          case s"transactions_${dateKey}.data" => {
            println(s"${file.name}: ${OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")
            readTxAndGenerateTopFilesPerStore(dateKey, file)
          }
          case _ => ()
        }
      }

    println(s"Per day, store->transactions rankings: ${OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")

    // On 7 days, store->transactions ranking
    dataFolder.children
      .flatMap { file =>
        file.name match {
          case s"reference_prod-${store}_${dateKey}.data" => Some(store)
          case _ => None
        }
      }
      .distinct // get distinct store ids
      .foreach { store =>
        outputSortedForDateRange(initialDate.minus(daysNumber, ChronoUnit.DAYS), initialDate, "ventes", (a, b) => a.qty > b.qty, store)
        outputSortedForDateRange(initialDate.minus(daysNumber, ChronoUnit.DAYS), initialDate, "ca", (a, b) => a.ca > b.ca, store)
      }

    println(s"On 7 days, store->transactions ranking: ${OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")

    // Per day, global rankings
    (0 to daysNumber).foreach { dayOffset =>
      val date = initialDate.minus(dayOffset, ChronoUnit.DAYS)
      outputSortedForDateRange(date, date, "ventes", (a, b) => a.qty > b.qty)
      outputSortedForDateRange(date, date, "ca", (a, b) => a.ca > b.ca)
    }

    println(s"Per day, global rankings: ${OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")

    // On 7 days, global rankings
    outputSortedForDateRange(initialDate.minus(daysNumber, ChronoUnit.DAYS), initialDate, "ventes", (a, b) => a.qty > b.qty)
    outputSortedForDateRange(initialDate.minus(daysNumber, ChronoUnit.DAYS), initialDate, "ca", (a, b) => a.ca > b.ca)

    println(s"On 7 days, global rankings: ${OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")

    println(s"Started: ${started.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")
    println(s"Ended: ${OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}")
  }

  /**
   * Read a transaction file entirely, group transaction lines by store.
   * For each store (parallelized)
   *    Reduce transactions on same product to a single line by summing quantities
   *    Join result with the store referential (read entirely)
   *    Sort result by descending CA and Qty and write the two sorted files
   * @param dateKey
   * @param file
   */
  private def readTxAndGenerateTopFilesPerStore(dateKey: String, file: File) = {
    file
      .lineIterator
      .flatMap {
        case s"$txId|$datetime|${storeId}|$product|$qty" => Some(TxLine(storeId, product, qty.toInt))
        case _ => None
      }
      .toList
      .groupBy { _.store }
      .map { case store -> txLines =>
        Future {
          // load store referential into memory
          val refMap = File(s"${dataFolder.pathAsString}/reference_prod-${store}_${dateKey}.data").lineIterator
            .map {
              case s"$product|$price" => product -> price.toFloat
            }.toMap

          // group transactions by product, reduce them to a single transaction line by summing quantities
          val reducedTxs = txLines.groupBy(_.product).values
            .map(txsPerProduct => txsPerProduct.reduce((a, b) => TxLine(store, a.product, a.qty + b.qty)))

          // join transactions and referential on productId
          val aggregates = reducedTxs.map { txLine =>
            val price = refMap.getOrElse(txLine.product, 0f)
            Aggregate(txLine.product, txLine.qty, txLine.qty * price)
          }.toList

          // output sorted by qty/ca
          outputSorted(dateKey, store, aggregates, "ca", (a, b) => a.ca > b.ca)
          outputSorted(dateKey, store, aggregates, "ventes", (a, b) => a.qty > b.qty)
        }
      }
      .toList
      .foreach(fu => Await.result(fu, Duration.Inf))
  }

  /**
   * Sort the aggregates with the given sortCriteria and take the first N elements where N is the limit
   * Output the result to a file
   * @param dateKey
   * @param storeId
   * @param aggregates
   * @param category
   * @param sortCriteria
   * @param limit
   * @return
   */
  private def outputSorted(dateKey: String,
                           storeId: String,
                           aggregates: List[Aggregate],
                           category: String,
                           sortCriteria: (Aggregate, Aggregate) => Boolean,
                           limit: Int = 100) = {
    val output = File(getTopFileName(dateKey, storeId, category, limit) + ".data").createIfNotExists()

    // print large number of lines that may not fit in memory
    aggregates.sortWith(sortCriteria)
      .take(limit)
      .map {
        case Aggregate(product, qty, ca) => s"$product|$qty|$ca"
      } |> { output.printLines(_) }
  }

  /**
   * Collect files that :
   * - Are in the date range
   * - Are of the given category
   * - Have the given scope (Global or storeId)
   * Then sort the aggregates with the sortCriteria and take the first N elements where N is the limit.
   * Output result to a file.
   * @param begDate
   * @param endDate
   * @param category
   * @param sortCriteria
   * @param scope
   * @param limit
   */
  private def outputSortedForDateRange(begDate: LocalDate,
                                       endDate: LocalDate,
                                       category: String,
                                       sortCriteria: (Aggregate, Aggregate) => Boolean,
                                       scope: String = "GLOBAL",
                                       limit: Int = 100) {

    val endDateKey = endDate.format(DateTimeFormatter.BASIC_ISO_DATE)
    val rangeDays = (endDate.getDayOfYear - begDate.getDayOfYear)

    val topFileName = getTopFileName(endDateKey, scope, category, limit)
    val file = if (rangeDays > 0)
      File(s"${topFileName}-J-${rangeDays+1}.data")
    else
      File(s"$topFileName.data")

    outputDir.children
      .flatMap { file =>
        file.name match {
          // file already processed, we skip it
          case s"top_${limit}_${suffix}_${storeId}_$dateKey-${range}.data" => None
          // file is read if date / scope are in range and the suffix matches
          case s"top_${_limit}_${suffix}_${storeId}_$dateKey.data" =>
            if (category == suffix && (scope == "GLOBAL" || scope == storeId) && limit == _limit.toInt) {
              val localDate = LocalDate.parse(dateKey, DateTimeFormatter.BASIC_ISO_DATE)
              if ((localDate.isAfter(begDate) && localDate.isBefore(endDate)) || localDate.isEqual(endDate))
                Some(file)
              else None
            }
            else None
          case _ => None
        }
      }
      // flatten file contents
      .flatMap(_.lines)
      // rebuild aggregates for sorting
      .flatMap {
        case s"$product|$qty|$ca" => Some(Aggregate(product, qty.toInt, ca.toFloat))
        case _ => None
      }
      .toList
      .sortWith {
        sortCriteria
      }
      .take(limit)
      // destructure to string for output
      .map {
        case Aggregate(product, qty, ca) => s"$product|$qty|$ca"
      } |> { file.printLines(_) }
  }

  private def getTopFileName(dateKey: String, storeId: String, category: String, limit: Int) = {
    s"${outputDirPath}/top_${limit}_${category}_${storeId}_$dateKey"
  }


  implicit class Pipe[T](val v: T) extends AnyVal {
    def |>[U] (f: T => U) = f(v)
  }
}