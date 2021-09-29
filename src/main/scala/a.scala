import scala.collection.mutable.ArrayBuffer

import scala.util.Try
import purecsv.unsafe.CSVReader

object Diss {

  case class GroupIterator[T, K, V](iter: Iterator[T], split: T => (K, V), reduce: (V, V) => V) extends Iterator[(K, V)] {
    var currentOutputKey: Option[K] = None
    var currentKey: Option[K] = None
    var currentValue: Option[V] = None
    var localValue: Option[V] = None
    val queue = scala.collection.mutable.Queue[(K, V)]()

    def read(): (Option[K], Option[V]) = {
      if (iter.hasNext) {
        val r = iter.next()
        val (key, value) = split(r)

        (Some(key), Some(value))
      } else {
        (None, None)
      }
    }

    override def hasNext: Boolean = {
      if (!iter.hasNext && queue.isEmpty) {
        if (currentOutputKey.isDefined) {
          queue.enqueue((currentOutputKey.get, currentValue.get))
          currentOutputKey = None
          true
        } else {
          false
        }
      } else if (!iter.hasNext && queue.nonEmpty) {
        true
      } else {
        if (currentOutputKey.isEmpty) {
          val (key, value) = read()
          currentKey = key
          currentOutputKey = key
          currentValue = value
        }

        while (currentKey.isDefined && currentOutputKey == currentKey) {
          val (key, value) = read()
          currentKey = key
          localValue = value
          if (value.isDefined && currentOutputKey == currentKey) {
            currentValue = currentValue.map(x => reduce(x, value.get))
          }
        }

        queue.enqueue((currentOutputKey.get, currentValue.get))
        currentOutputKey = currentKey
        currentValue = localValue

        true
      }
    }

    override def next(): (K, V) = {
      queue.dequeue()
    }
  }

  class UnionIterator[A](iterators: Seq[Iterator[A]]) extends Iterator[A] {
    // scalastyle:off
    var idx = 0
    var currentIterator: Option[Iterator[A]] = iterators.headOption
    // scalastyle:on

    override def hasNext: Boolean = {
      if (currentIterator.isDefined && currentIterator.get.hasNext) {
        true
      } else {
        while (idx < iterators.length && !iterators(idx).hasNext) {
          idx += 1
        }

        if (idx < iterators.length && iterators(idx).hasNext) {
          currentIterator = Some(iterators(idx))
          true
        } else {
          false
        }
      }
    }

    override def next(): A = currentIterator.get.next()
  }

  import java.io._
  import java.nio.file.{Files, Paths}

  import scala.reflect.io
  import scala.reflect.io.Directory

  /**
    * Utility object for File access.
    */
  object FileUtils {

    type FileName = String
    type Dir = String

    def withFile[A](fileName: FileName)(func: PrintWriter => A): Unit = {
      val file = new File(fileName)
      val write = new PrintWriter(file)
      try {
        func(write)
      } finally {
        write.close()
      }
    }

    /**
      * Directory list
      *
      * @param dir dir
      * @return only filenames, i.e. "/tmp/1/2/3" -> "3"
      */
    def list(dir: Dir): List[FileName] = {
      filesInDir(dir).map(_.name).toList
    }

    /**
      * Full directory list
      *
      * @param dir dir
      * @return full path filenames, i.e. "/tmp/1/2/3"
      */
    def fullList(dir: Dir): List[FileName] = {
      list(dir).map(fileName => s"$dir/$fileName")
    }

    def fromFile(filePath: FileName, encoding: String = "iso-8859-1"): Iterator[String] = scala.io.Source.fromFile(filePath, encoding).getLines

    def readFile(filePath: FileName, encoding: String = "iso-8859-1"): String = fromFile(filePath, encoding).mkString("\n")

    def readBinaryFile(fileName: FileName): Array[Byte] = {
      Files.readAllBytes(Paths.get(fileName))
    }

    def filesInDir(dir: Dir, fileNameFilter: (FileName => Boolean) = (x => true)): Array[io.File] = {
      Directory(dir).files.toArray.filter(file => fileNameFilter(file.name)).sortBy(x => x.name)
    }

    // scalastyle:off regex
    def write(fileName: FileName, iterator: Iterator[String]): Unit = {
      withFile(fileName) { output =>
        iterator.foreach(line => output.println(line))
      }
    }

    // scalastyle:on regex

    def write(fileName: FileName, value: String): Unit = {
      write(fileName, Iterator.single(value))
    }

    def write(fileName: FileName, array: Array[Byte]): Unit = {
      import java.io.FileOutputStream
      val fos = new FileOutputStream(fileName)
      fos.write(array)
      fos.close()
    }

    def write(fileName: FileName, stream: InputStream): Unit = {
      Files.copy(stream, new java.io.File(fileName).toPath)
    }

    def copyFile(srcPath: String, destPath: String): Unit = {
      val src = new File(srcPath)
      val dest = new File(destPath)
      new FileOutputStream(dest).getChannel.transferFrom(
        new FileInputStream(src).getChannel, 0, Long.MaxValue)
    }

    def exist(path: String): Boolean = {
      new java.io.File(path).exists
    }

    def delete(fileName: FileName): Boolean = {
      new File(fileName).delete()
    }

    def deleteNonEmptyDir(dir: Dir): Boolean = {
      filesInDir(dir).foreach(x => delete(x.path))
      new Directory(new File(dir)).delete()
    }

    def fileSize(fileName: FileName): Long = {
      new File(fileName).length()
    }

    def appendLine(fileName: FileName, value: String): Unit = {
      val fileWriter = new FileWriter(fileName, true)
      try {
        fileWriter.write(value)
        fileWriter.write("\n")
      } finally {
        fileWriter.close()
      }
    }
  }

  lazy val all: Map[String, Int] = scala.io.Source.fromFile("all").getLines().drop(1).map(_.split(" ").last).toList.zipWithIndex.toMap
  lazy val allSet = all.keysIterator.map(_.split("\\.").head).toSet
  lazy val all_back: Map[Int, String] = scala.io.Source.fromFile("all").getLines().drop(1).map(_.split(" ").last).toList.zipWithIndex.map(x => (x._2, x._1)).toMap
  lazy val loc: Map[String, String] = scala.io.Source.fromFile("copy").getLines().map(_.split(" ").last).map { x =>
    val split = x.split("/")
    (split(1), split(0))
  }.toMap

  case class MetaData2(id: String, _type: String, city: String, title: String, author: String, year: String, specs: String, grade: String, sciArea: String, univer: String, board: String) {
    lazy val y: Int = Try {
      val res = year.toInt
      assert(res > 1970 && res < 2021)
      res
    }.getOrElse(0)

    def lastName = author.split(" ").head.toUpperCase

    def firstName = author.split(" ").drop(1).mkString(" ").toUpperCase
  }


  object MetaData2 {
    lazy val err: MetaData2 = MetaData2("", "", "", "", "", "", "", "", "", "", "")

    def fromString(value: String): MetaData2 = {
      Try {
        CSVReader[MetaData2].readCSVFromString(value, delimiter = ',').head
      }.getOrElse(err)
    }

    def fromFile(fileName: String): Iterator[MetaData2] = {
      scala.io.Source.fromFile(fileName, "UTF-8").getLines().drop(1).map(fromString)
    }
  }

  lazy val meta2: Map[String, MetaData2] = MetaData2.fromFile("text.csv").filter(x => allSet.contains(x.id)).map(x => (x.id, x)).toMap

  def getDissMeta(id: Int): MetaData2 = {
    meta2(all_back(id).dropRight(4))
  }


  def jaccardBySize(fst: String, snd: String, windowSize: Int): Double = {
    val fs = fst.sliding(windowSize, 1).toSet
    val ss = snd.sliding(windowSize, 1).toSet

    val d = (fs ++ ss).size
    if (d == 0) {
      1
    } else {
      (fs intersect ss).size.toDouble / d
    }
  }

  def jaccard(fst: String, snd: String): Double = {
    jaccardBySize(norm(fst), norm(snd), 3)
  }

  def norm(value: String): String = {
    value.filter(_.isLetter).toUpperCase
  }

  lazy val disBad: Map[(String, Int), List[((String, Int), String)]] = scala.io.Source.fromFile("DISBASE.txt").getLines().toList.map(x => x.replace("\t", "")).map { x =>
    Try {
      val split = x.split("\\|").toList
      val lastName = split(1).toUpperCase
      val year = Try {
        split(0).takeRight(4).toInt
      }.getOrElse(split(27).takeRight(4).toInt)
      ((lastName, year), x)
    }.getOrElse((("", 0), ""))
  }.groupBy(x => x._1).withDefaultValue(Nil)


  def extractWorkNamefromDisbase(value: String): String = {
    (value + " ").split("\\|")(4)
  }

  def extractSpecfromDisbase(value: String): String = {
    (value + " ").split("\\|").last
  }

  def onlyNum(value: String): String = value.filter(_.isDigit)

  def findDisBad(meta: MetaData2): String = {
    disBad((meta.lastName, meta.y)).find { t =>
      (jaccard(extractWorkNamefromDisbase(t._2), meta.title) > 0.8) || (onlyNum(extractSpecfromDisbase(t._2)) == onlyNum(meta.board)
        )
    }.map(_.toString()).getOrElse("")
  }


  lazy val falseData: Map[String, String] = scala.io.Source.fromFile("falsedat.txt", "UTF-8").getLines().map { line =>
    val split = line.split("//")
    val id = split.head.trim
    val value = split.drop(1).mkString("//")
    (id, value)
  }.toMap.withDefaultValue("")

  lazy val cyberMeta: Map[String, Int] = scala.io.Source.fromFile("down/meta.c").getLines().map { line =>
    val split = line.split("\t")
    (split.last, split.head.toInt)
  }.toMap

  lazy val cyberMetaRev: Map[Int, String] = cyberMeta.toList.map(x => (x._2, x._1)).toMap

  lazy val cyberMeta2: Map[String, (String, Int)] = scala.io.Source.fromFile("down/total", "UTF-8").getLines().filter { line =>
    val split = line.split("\t")
    cyberMeta.contains(s"${split.head.split("/").last}.txt")
  }.map { line =>
    val split = line.split("\t")
    (s"${split.head.split("/").last}.txt", (split(1), split(2).toInt))
  }.toMap.withDefaultValue(("", 2048))


  def getCyberMeta(id: Int): (String, Int) = {
    cyberMeta2(cyberMetaRev(id))
  }

  lazy val forbidden = scala.io.Source.fromFile("dubfiles").getLines().flatMap(_.split("\t").drop(2).toList).map(_.split("/").last).map(x => all(x)).toSet
  lazy val forbidden1 = scala.io.Source.fromFile("dubfiles").getLines().flatMap(_.split("\t").drop(2).toList).map(_.split("/").last).toSet


  object BaseRef1 {
    def year(baseRef: BaseR): Int = {
      baseRef match {
        case DissRef(id) => meta2(all_back(id).dropRight(4)).y
        case CyberRef(id) =>
          val path = cyberMetaRev(id)
          cyberMeta2(path)._2
      }
    }

    def getLastName(dissRef: DissRef): String = {
      meta2(all_back(dissRef.id).dropRight(4)).lastName
    }

    def diffLastName(baseRef: BaseR, lastName: String): Boolean = {
      baseRef match {
        case x@DissRef(id) => getLastName(x) != lastName
        case CyberRef(id) =>
          val path = cyberMetaRev(id)
          !cyberMeta2(path)._1.contains(lastName)
      }

    }
  }

  sealed trait BaseR {}

  case class DissRef(id: Int) extends BaseR {}

  case class CyberRef(id: Int) extends BaseR {}

  def fromId(id: Int): BaseR = {
    if (id < 0) {
      CyberRef(-id)
    } else {
      DissRef(id)
    }
  }

  def extraGraphNg(mainHash: Set[String], items: Map[BaseR, Set[String]], threshold: Int = 150): List[(BaseR, Int)] = {
    val fold = (1 to items.size).foldLeft[(Set[String], Set[BaseR], List[(BaseR, Int)])]((mainHash, Set[BaseR](), Nil)) {
      case ((set, used, acc), _) =>

        val max: (BaseR, Set[String]) = items.filterKeys(k => !used.contains(k)).maxBy(_._2.intersect(set).size)

        (set -- max._2, used + max._1, acc :+ (max._1, set.intersect(max._2).size))
    }

    fold._3.filter(_._2 >= threshold)
  }

  object RefInfo {
    def getShortInfo(baseR: BaseR): String = {
      baseR match {
        case DissRef(id) => s"Diss(${all_back(id).dropRight(4)})"
        case CyberRef(id) => s"Cyber($id)"
      }
    }

    def getLongInfo(baseR: BaseR): String = {
      baseR match {
        case DissRef(id) => s"Diss(${getDissMeta(id)})"
        case CyberRef(id) =>
          val link = s"https://cyberleninka.ru/article/n/${cyberMetaRev(id).dropRight(4)}"
          val cmeta = cyberMeta2(cyberMetaRev(id))
          s"Cyber($id, $cmeta, $link)"
      }
    }

  }

  def extraDotGraph(main: BaseR, mainHash: Set[String], items: Map[BaseR, Set[String]]): String = {
    val alls: Map[BaseR, Set[String]] = items + (main -> mainHash)

    val edges = alls.keySet.toList.combinations(2).toList.flatMap { pair: List[BaseR] =>
      val fst = pair.head
      val snd = pair.last
      val size = (alls(fst) intersect alls(snd)).size
      if (size >= 150) {
        val (f, s, ar, ex) = (BaseRef1.year(fst), BaseRef1.year(snd)) match {
          case (a, b) if a < b => (fst, snd, "->", "")
          case (a, b) if a > b => (snd, fst, "->", "")
          case (a, b) if a == b => (snd, fst, "->", "dir=both")
        }
        Some(s""" "${RefInfo.getShortInfo(f)}" $ar "${RefInfo.getShortInfo(s)}" [label="$size" $ex]; """)
      } else {
        None
      }
    }.mkString("\n")

    s"""
       |digraph G {
       |  {
       |    node [];
       |    "${RefInfo.getShortInfo(main)}" [shape = polygon];
       |  }
       |
       |  $edges
       |}
       |""".stripMargin.replace("\n", "")
  }

  def readHash(file: String, id: Int): Set[String] = {
    Try {
      val iter = scala.io.Source.fromFile(file).getLines().flatMap { line =>
        val split = line.split("\t")
        val idx = split.head.toInt
        if (idx == id) {
          Some(split.drop(1).toSet)
        } else {
          None
        }
      }

      iter.take(1).toList.headOption.getOrElse(Set())
    }.getOrElse(Set())
  }


  def getHashes(baseR: BaseR): Set[String] = {
    val (id, fileName) = baseR match {
      case DissRef(id) => (id, s"ddd/${id % 1000}/$id.ddd")
      case CyberRef(id) => (id, s"ccc/${id % 1000}/$id.ccc")
    }

    readHash(fileName, id)
  }

  sealed trait TableSource {}

  case class DissTableRef(diss: DissRef, page: Int) extends TableSource

  case class CyberTableRef(diss: CyberRef) extends TableSource

  def normalizeLastName(ln: String): String = {
    ln.replace("Ё", "Е")
  }

  object LastName {
    def fromOne(value: String): LastName = {
      LastName(List(value))
    }
  }

  case class LastName(values: List[String]) extends {

    override def equals(o: Any): Boolean = o match {
      case that: LastName =>
        (this.values, that.values) match {
          case (a :: Nil, b :: Nil) => a == b
          case (x@(a :: a1 :: a2 :: Nil), b :: Nil) => x.contains(b)
          case (b :: Nil, x@(_ :: _ :: _ :: Nil)) => x.contains(b)
          case (y@(_ :: _ :: _ :: Nil), x@(_ :: _ :: _ :: Nil)) => x.sorted == y.sorted
          case (a, b) => val (min, max) = if (a.size < b.size) {
            (a, b)
          } else {
            (b, a)
          }
            max.toSet.intersect(min.toSet) == min.toSet
        }
      case _ => false
    }

    override def hashCode = 0
  }


  def getLastNames(b: BaseR): Set[LastName] = {
    b match {
      case x@DissRef(id) => Set(LastName.fromOne(normalizeLastName(BaseRef1.getLastName(x))))
      case CyberRef(id) =>
        val path = cyberMetaRev(id)
        val f = cyberMeta2(path)._1.replace(".", " ")
        f.split(",").map(x => x.split(" ").filter(_.size > 1).toList).map(x => x.map(normalizeLastName)).map(x => LastName(x)).toSet
    }
  }

  def pp(a: (Int, Int), b: (Int, Int)): (Int, Int) = {
    (a._1 + b._1, a._2 + b._2)
  }

  def countSource(source: List[BaseR]): (Int, Int) = {
    source.map {
      case x: DissRef => (1, 0)
      case x: CyberRef => (0, 1)
    }.reduce((a, b) => pp(a, b))
  }


  def typeClass(mainSet: Set[LastName], others: Set[LastName], cp: List[BaseR]): Int = {
    val (d, c) = countSource(cp)
    (mainSet, others) match {
      case (a, b) if (a intersect b).isEmpty => 1
      case (a, b) if a == b && d > 0 && c > 0 => 30
      case (a, b) if a == b && d > 0 && c == 0 => 31
      case (a, b) if a == b && d == 0 && c > 0 => 32
      case (a, b) if (a intersect b).nonEmpty => 2
      case _ => 0
    }
  }

  // fileName (wo .txt) -> (journal, vak_label)
  lazy val cyberTitle: Map[String, (String, String)] = scala.io.Source.fromFile("cyber.title", "UTF-8").getLines().map { line =>
    val split = (line + " ").split("\t")
    split(0) -> (split(1), split(2).dropRight(1))
  }.toMap.withDefaultValue(("???", "???"))

  //id -> size
  lazy val cyberSize: Map[Int, Int] = scala.io.Source.fromFile("cyber.size", "UTF-8").getLines().map { line =>
    val split = line.split("\t").map(_.toInt)
    split.head -> split.last
  }.toMap.withDefaultValue(0)

  def getCyberJournal(c: CyberRef): String = {
    cyberTitle(cyberMetaRev(c.id).dropRight(4))._1
  }

  def getCyberLabel(c: CyberRef): String = {
    cyberTitle(cyberMetaRev(c.id).dropRight(4))._2
  }

  def getCyberSize(c: CyberRef): Int = {
    cyberSize(c.id)
  }

  lazy val validCyberIds: Set[Int] = scala.io.Source.fromFile("valid.cyber").getLines().map(_.toInt).toSet

  def getRef(tableSource: TableSource): BaseR = {
    tableSource match {
      case CyberTableRef(diss) => diss
      case DissTableRef(diss, _) => diss
    }
  }

  def isPlagiat(fst: BaseR, snd: BaseR): Boolean = {
    val f = getLastNames(fst)
    val s = getLastNames(snd)
    (f intersect s).isEmpty
  }

  def getTsvLongInfo(baseR: BaseR): List[String] = {
    baseR match {
      case x@DissRef(id) => List[String](BaseRef1.year(baseR).toString, BaseRef1.getLastName(x), RefInfo.getLongInfo(x))
      case x@CyberRef(id) => List[String](BaseRef1.year(baseR).toString, getCyberMeta(id)._1, getCyberJournal(x), getCyberLabel(x), RefInfo.getLongInfo(x))
    }
  }

  lazy val tableCl: Set[Int] = scala.io.Source.fromFile("clTable.id").getLines.map(_.toInt).toSet

  def clHasTable(cyberRef: CyberRef): Boolean = {
    tableCl.contains(cyberRef.id)
  }

}