package diss

object Norm {
  lazy val replacement: scala.collection.Map[Char, String] = Map(
    '\ufb00' -> "ff",
    '\ufb01' -> "fi",
    '\ufb02' -> "fl",
    '\ufb03' -> "ffi",
    '\ufb04' -> "ffl",
    '\ufb05' -> "ft",
    '\ufb06' -> "st",
  )

  def replace(value: String): String = {
    value.flatMap(c => if (replacement.contains(c)) replacement(c) else c.toString)
  }

  def stringNorm(value: String): String = {
    replace(value).toUpperCase.map(c => if (c.isLetter) c else ' ').split(" ").filter(_.nonEmpty).mkString(" ")
  }

  def toNgram(value: String): Vector[String] = {
    stringNorm(value).split(" ").sliding(6).filter(_.map(_.size).sum > 15).map(_.mkString(" ")).toVector
  }

  def hash(str: String, seed: Int): Int = {
    var res = 1
    for (c <- str) {
      res = res * seed + c.toInt
    }

    res
  }

  def hash64(str: String): (Int, Int) = {
    hash(str, 5) -> hash(str, 7)
  }

  def read(fileName: String): String = {
    import java.io._
    val br = new BufferedReader(new InputStreamReader(
      new FileInputStream(fileName), "UTF-8"))
    val res = br.lines().toArray.toList.map(_.toString).mkString(" ")
    br.close()
    res
  }

  def hashReadDir(dirPath: String, fileNameExtractor: String => String, output: String) = {
    val iter = FileUtils.filesInDir(dirPath).iterator.flatMap { fileName =>
      val fileId = fileNameExtractor(fileName.name)
      val content = read(fileName.path)
      toNgram(content).map { ng =>
        val h = hash64(ng)
        s"${h._1}\t${h._2}\t${fileId}"
      }
    }

    FileUtils.write(output, iter)
  }
}

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val pathPrefix = args.head
    val bibIdx = args(1)

    println(s"$pathPrefix $bibIdx")
    FileUtils.dirsInDir(pathPrefix).par.foreach(dir => Norm.hashReadDir(pathPrefix + "/" + dir.toAbsolute.name, s => bibIdx + s.split("/").last.filter(_.isDigit), s"${dir.name}.th"))
  }
}
