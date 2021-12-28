package web

import ujson.{Obj, Value}

object Web extends cask.MainRoutes {
  override def port: Int = 8081

  override def host: String = "0.0.0.0"

  @cask.get("/")
  def hello(): String = {
    "Hello World!"
  }

  @cask.staticFiles("/static/:path")
  def staticFileRoutes(path: String) = "static/" + path

  @cask.postJson("/search")
  def search(data: ujson.Value): Value = {
    val str = data.str

    val pages: Vector[(DissPageItem, Int)] = NumSearch.find(str).map { case (x, cnt) =>
      DissPageItem(RgbSearch.find(x.id).getOrElse(RgbSearch.empty), x.page) -> cnt
    }

    val meta: Vector[(String, String)] = pages.map(_._1.rgbId).distinct.map {
      rgbId => rgbId -> MetaDataSearch.find(rgbId).getOrElse(MetaDataSearch.empty)
    }

    val byPageJson: Vector[Obj] = pages.map { case (item, cnt) =>
      ujson.Obj("rgbId" -> item.rgbId,
        "page" -> item.page,
        "cnt" -> cnt
      )
    }

    val metaJson: Vector[Obj] = meta.map { case (rgbId, m) =>
      ujson.Obj("rgbId" -> rgbId,
        "metaData" -> m
      )
    }

    ujson.Obj(
      "byPage" -> byPageJson,
      "meta" -> metaJson
    )
  }

  initialize()
}

trait TableItem

case class DissPage(id: Int, page: Int) extends TableItem

case class DissPageItem(rgbId: String, page: Int)

object NumSearch {
  val fileName = "data/diss.num.tt"

  def normalize(str: String): List[String] = {
    val list = str.filterNot(x => x == ',' || x == '.').map(x => if (x.isDigit) x else ' ').split(" ").filter(_.nonEmpty).toList
    val res = list.filter { x =>
      (x.length < 10 && ((x.toLong < 1900 && x.toLong > 10) || (x.toLong > 2030)) || x.startsWith("0"))
    }.filterNot(x => x.forall(_ == '0'))
    if (res.size <= 2) {
      List()
    } else {
      res
    }
  }

  def hash(str: String, seed: Int): Int = {
    var res = 1
    for (c <- str) {
      res = res * seed + c.toInt
    }

    res
  }

  def find(value: String): Vector[(DissPage, Int)] = {
    val numbers = normalize(value)

    val alls: Vector[DissPage] = numbers.sliding(3, 1).toVector.flatMap { triple =>
      val str = triple.mkString(" ")
      val lookFor = s"${hash(str, 5)}\t${hash(str, 7)}\t"

      BinSearch.find(fileName, lookFor).toVector.flatMap { offset =>
        val lines = BinSearch.read(fileName, offset, lookFor)
        lines.map { line =>
          val split = line.split("\t")
          DissPage(split(2).toInt, split(3).toInt)
        }
      }
    }

    alls.groupBy(identity).view.mapValues(_.size).toVector.sortBy(_._2)(Ordering[Int].reverse).take(20)
  }
}

object RgbSearch {
  val empty: String = "00000000000"
  val fileName = "data/mapping.id.rgb"

  def find(id: Int): Option[String] = {
    val lookFor = s"$id\t"
    BinSearch.find(fileName, lookFor).flatMap { offset =>
      val lines = BinSearch.read(fileName, offset, lookFor)
      lines.headOption.map(_.split("\t").last)
    }
  }
}

object MetaDataSearch {
  val empty: String = "NOT_FOUND"
  val fileName = "data/text.csv.s"

  def find(rgbId: String): Option[String] = {
    val lookFor = s"$rgbId,"
    BinSearch.find(fileName, lookFor).flatMap { offset =>
      val lines = BinSearch.read(fileName, offset, lookFor)
      lines.headOption.map { line =>
        line.drop(lookFor.size)
      }
    }
  }
}
