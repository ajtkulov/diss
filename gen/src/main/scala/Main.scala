package diss

import java.nio.ByteBuffer

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
    stringNorm(value).split(" ").sliding(6).filter(_.map(_.size).sum > 15).map(_.mkString(" ")).toVector.distinct
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

  def hashReadDir(dirPath: String, fileNameExtractor: String => String, output: String): Unit = {
    val iter = FileUtils.filesInDir(dirPath).iterator.flatMap { fileName =>
      val fileId = fileNameExtractor(fileName.name)
      val content = read(fileName.path)
      toNgram(content).flatMap { ng =>
        val h = hash64(ng)
        if (Math.abs(h._1 + h._2) % 10 == 0) {
          Some(s"${h._1}\t${h._2}\t${fileId}")
        } else {
          None
        }
      }
    }

    FileUtils.write(output, iter)
  }

  def hashReadDirByte(dirPath: String, fileNameExtractor: String => Int, output: String): Unit = {
    val iter: Iterator[Array[Byte]] = FileUtils.filesInDir(dirPath).iterator.flatMap { fileName =>
      val fileId = fileNameExtractor(fileName.name)
      val content = read(fileName.path)
      toNgram(content).flatMap { ng =>
        val h = hash64(ng)
        if (Math.abs(h._1 + h._2) % 10 == 0) {
          Some(int2bytes(Vector[Int](h._1, h._2, fileId)))
        } else {
          None
        }
      }
    }

    FileUtils.writeBytes(output, iter)
  }


  def int2bytes(values: Vector[Int]): Array[Byte] = {
    val buffer = ByteBuffer.allocate(values.size * 4)
    values.foreach { value =>
      buffer.putInt(value)
    }
    buffer.array()
  }

  def extractNumPm(str: String): List[String] = {
    val main = str.replaceAll("[.,]", "").map(c => if (c.isDigit || c == 177.toChar) c else ' ')
    val split = main.split(177.toChar)
    val list = split.map(x => x.split(" ").filter(_.nonEmpty))
    (for {i <- 1 until list.size
          if list(i - 1).size >= 2 && list(i).size >= 2 && list(i - 1).last.length > 1 && list(i).head.length > 1
          } yield s"${list(i - 1).last}/${list(i).head}").toList.distinct
  }

  def hashReadPmTableDir(dirPath: String, fileNameExtractor: String => String, output: String): Unit = {
    val iter = FileUtils.filesInDir(dirPath).iterator.flatMap { fileName =>
      val fileId = fileNameExtractor(fileName.name)
      val content = read(fileName.path)
      val zipPages = content.split(12.toChar).zipWithIndex

      zipPages.flatMap { case (page, num) =>
        val list = extractNumPm(page)
        if (list.size >= 6) {
          list.map { pm =>
            s"$pm\t$fileId:$num"
          }
        } else {
          List.empty
        }
      }
    }

    FileUtils.write(output, iter)
  }

}

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val pathPrefix = args.head
    println(s"$pathPrefix")
    FileUtils.dirsInDir(pathPrefix).par.foreach(dir => Norm.hashReadDirByte(pathPrefix + "/" + dir.toAbsolute.name, s => s.split("/").last.filter(_.isDigit).toInt, s"${dir.name}.th"))
  }
}

object MainPMTable extends App {
  override def main(args: Array[String]): Unit = {
    val pathPrefix = args.head
    val bibPrefix = args(1)
    println(s"$pathPrefix $bibPrefix")
    FileUtils.dirsInDir(pathPrefix).par.foreach(dir => Norm.hashReadPmTableDir(pathPrefix + "/" + dir.toAbsolute.name, s => bibPrefix + s.split("/").last.filter(_.isDigit), pathPrefix + "/" + s"${dir.name}.pm"))
  }
}
