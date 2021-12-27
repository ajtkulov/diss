package web

object Web extends cask.MainRoutes {
  @cask.get("/")
  def hello() = {
    "Hello World!"
  }

  @cask.postJson("/search")
  def doThing(data: ujson.Value) = {
    val str = data.str

    str
  }

  initialize()
}

object NumSearch {
  val fileName = "diss.num.tt"

  def normalize(str: String): List[String] = {
    val list = str.filterNot(x => x == ',' || x == '.').map(x => if (x.isDigit) x else ' ').split(" ").filter(_.nonEmpty).toList
    val res = list.filter { x =>
      (x.length < 10 && ((x.toLong < 1900 && x.toLong > 10) || (x.toLong > 2030)) || x.startsWith("0"))
    }.filterNot(x => x.forall(_ == '0'))
    if (res.size <= 3) {
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


  def find(value: String) = {
    val numbers = normalize(value)

    numbers.sliding(3, 1).map { triple =>
      val str = triple.mkString(" ")
      val lookFor = s"${hash(str, 5)}\t${hash(str, 7)}\t"

      BinSearch.find(fileName, lookFor)
    }
  }
}