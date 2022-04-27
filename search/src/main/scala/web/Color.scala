package web

import scala.collection.mutable.ArrayBuffer

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

  def color(fst: String, snd: String): (String, String) = {
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

    val ff1: List[((String, Int), Boolean)] = fn1.zipWithIndex.map(x => x -> fres.contains(x._2)).toList

    val ss1: List[((String, Int), Boolean)] = sn1.zipWithIndex.map(x => x -> sres.contains(x._2)).toList

    val fcolor: List[String] = grouping[((String, Int), Boolean)](ff1, (a, b) => a._2 == b._2).map { list =>
      if (list.head._2) {
        s"<span style='background-color:#B2A6DA'>${list.map(_._1._1).mkString(" ")}</span>"
      } else {
        list.map(_._1._1).mkString(" ")
      }
    }

    val scolor = grouping[((String, Int), Boolean)](ss1, (a, b) => a._2 == b._2).map { list =>
      if (list.head._2) {
        s"<span style='background-color:#B2A6DA'>${list.map(_._1._1).mkString(" ")}</span>"
      } else {
        list.map(_._1._1).mkString(" ")
      }
    }

    (fcolor.mkString(" "), scolor.mkString(" "))
  }

  def grouping[T](sorted: List[T], sameGroupFunc: (T, T) => Boolean): List[List[T]] = {
    if (sorted.isEmpty) {
      List()
    } else {
      val res = ArrayBuffer[ArrayBuffer[T]]()

      res.append(ArrayBuffer())
      res.last.append(sorted.head)
      for (item <- sorted.drop(1)) {
        if (sameGroupFunc(res.last.last, item)) {
          res.last.append(item)
        } else {
          res.append(ArrayBuffer())
          res.last.append(item)
        }
      }

      res.map(_.toList).toList
    }
  }


}


