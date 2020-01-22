package example

import java.io.File

import scala.io.Source
import scala.util.Using

case class AggregateTx(product: String, qty: Long, ca: Double)

object Main extends App {
  /**
   * Création des aggrégats :
   * - Pour chaque ref_magasin
   * Lire le ref_magasin
   * Lire les transactions ligne par ligne
   * Output la jointure transactions/ref_magasin dans un fichier magasin_datekey avec format (produit|qte|ca)
   * Vérifier si la ligne existe déjà => update qte et ca
   *
   */

  val path = getClass.getResource("/data")
  val dataFolder = new File(path.getPath)

  if (dataFolder.exists && dataFolder.isDirectory) {
    dataFolder.listFiles
      .map(_.getName)
      .flatMap { fileName =>
        fileName match {
          case s"reference_prod-${storeId}_${dateKey}.data" => {
            Some(processTx(dateKey, storeId, readRefFile(s"data/$fileName")))
          }
          case _ => None
        }
      }
      .foreach {
        aggregate => println(aggregate)
      }
  }

  private def readRefFile(path: String) = {
    Using.resource(Source.fromResource(path)) { source =>
      source.getLines().flatMap {
        case s"$product|$price" => Some(product -> price.toFloat)
        case _ => None
      }.toMap
    }
  }

  private def processTx(refDateKey: String, refStoreId: String, refMap: Map[String, Float]) = {

    Using.resource(Source.fromResource(s"data/transactions_${refDateKey}.data")) { source =>
      source.getLines().flatMap {
        case s"$txId|$datetime|$storeId|$product|$qty" => {
          if (storeId == refStoreId) {
            val qtyLong = qty.toLong;
            val productPrice = refMap.getOrElse(product, 0f)
            Some(AggregateTx(product, qtyLong, productPrice * qtyLong))
          } else None
        }
        case _ => None
      }.toList
    }
  }
}
