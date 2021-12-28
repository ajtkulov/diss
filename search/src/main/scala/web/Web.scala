package web

object Web extends cask.MainRoutes {
  override def port: Int = 8081

  override def host: String = "0.0.0.0"

  @cask.get("/")
  def hello(): String = {
    "Hello World!"
  }

  @cask.postJson("/search")
  def search(data: ujson.Value): String = {
    val str = data.str

    NumSearch.find(str).toString
  }

  initialize()
}

object NumSearch {
  trait TableItem

  case class DissPage(id: Int, page: Int) extends TableItem

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