package web

object Color {
  type Hash = (Int, Int)

  def hash(str: String, seed: Int): Int = {
    var res = 1
    for (c <- str) {
      res = res * seed + c.toInt
    }

    res
  }

  def hashT(str: String): Hash = {
    (hash(str, 5), hash(str, 7))
  }

  def upperFirstLetter(value: String) = {
    value.head.toUpper + value.tail
  }

  lazy val replaced: Vector[(String, String)] = scala.io.Source.fromFile("data/replace.csv", "UTF-8").getLines.flatMap { x =>
    val split = x.split("\t")
    List(split.head -> split.last,
      split.head.toLowerCase -> split.last.toLowerCase,
      upperFirstLetter(split.head.toLowerCase) -> upperFirstLetter(split.last.toLowerCase)
    )
  }.toVector.sortBy(_._1.size)(Ordering[Int].reverse).map(x => x._1.toList.mkString(" ") -> x._2)

  def norm(text: String): String = {
    replaced.foldLeft(text) { case (t, (from, to)) =>
      t.replace(from, to)
    }
  }


  def normalize(text: String): Vector[String] = {
    val output: String = replaced.foldLeft(text) { case (t, (from, to)) =>
      t.replace(from, to)
    }

    output.toUpperCase.map(c => if (c.isLetter) c else ' ').split(" ").filter(_.nonEmpty).toVector
  }

  def normalizeSimple(text: String): Vector[String] = {
    val output: String = replaced.foldLeft(text) { case (t, (from, to)) =>
      t.replace(from, to)
    }

    output.map(c => if (c.isLetter) c else ' ').split(" ").filter(_.nonEmpty).toVector
  }

  def color(fst: String, snd: String) = {
    val fn: Vector[String] = normalize(fst)
    val sn: Vector[String] = normalize(snd)

    val fn1: Vector[String] = normalizeSimple(fst)
    val sn1: Vector[String] = normalizeSimple(snd)

    val fs: Vector[(Int, Hash)] = fn.sliding(5).zipWithIndex.map(x => x._2 -> hashT(x._1.mkString(" "))).toVector
    val ss: Vector[(Int, Hash)] = sn.sliding(5).zipWithIndex.map(x => x._2 -> hashT(x._1.mkString(" "))).toVector

    val fset = fs.map(_._2).toSet
    val sset = ss.map(_._2).toSet

    val fidx = fs.filter(x => sset.contains(x._2)).map(_._1)
    val sidx = ss.filter(x => fset.contains(x._2)).map(_._1)

    val fres: Set[Int] = fidx.flatMap { idx =>
      Vector.range(idx, idx + 5)
    }.toSet

    val sres: Set[Int] = sidx.flatMap { idx =>
      Vector.range(idx, idx + 5)
    }.toSet

    val fcolor = fn1.zipWithIndex.map { case (w, idx) =>
      if (fres.contains(idx)) {
        s"<span style='background-color:#B2A6DA'>$w</span>"
      } else {
        w
      }
    }

    val scolor = sn1.zipWithIndex.map { case (w, idx) =>
      if (sres.contains(idx)) {
        s"<span style='background-color:#B2A6DA'>$w</span>"
      } else {
        w
      }
    }

    (fcolor.mkString(" "), scolor.mkString(" "))
  }

}


