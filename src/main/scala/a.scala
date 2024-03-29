import scala.util.Try
import purecsv.unsafe.CSVReader

import scala.annotation.tailrec
import scala.collection.mutable
import scala.math.min

object Diss {

  case class CountingIterator[T](iterator: Iterator[T], footstep: Int = 1000000) extends Iterator[T] {
    var count: Long = 0

    override def hasNext: Boolean = iterator.hasNext

    override def next(): T = {
      count = count + 1
      if (count % footstep == 0) {
        println(s"Iterator've moved ${count} steps.")
      }

      iterator.next()
    }
  }


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

    def traverse(dir: String): Vector[FileName] = {
      val names: Vector[FileName] = Directory(dir).dirs.toVector.map(_.toAbsolute.name).flatMap(d => traverse(s"$dir/$d"))
      fullList(dir).toVector ++ names
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

  lazy val meta2all: Vector[MetaData2] = MetaData2.fromFile("text.csv").toVector

  def getDissMeta(id: Int): MetaData2 = {
    Try {
      meta2(all_back(id).dropRight(4))
    }.getOrElse(MetaData2.err)
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
      val year = extractYearFromDisbase(x)
      ((lastName, year), x)
    }.getOrElse((("", 0), ""))
  }.groupBy(x => x._1).withDefaultValue(Nil)

  def extractYearFromDisbase(value: String): Int = {
    val split = value.split("\\|").toList
    val year = Try {
      Try {
        split(0).takeRight(4).toInt
      }.getOrElse(split(27).takeRight(4).toInt)
    }.getOrElse(0)
    year
  }

  def extractWorkNamefromDisbase(value: String): String = {
    (value + " ").split("\\|")(4)
  }

  def extractSpecfromDisbase(value: String): String = {
    (value + " ").split("\\|").last.trim.split(" ").last
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
      Try {
        meta2(all_back(dissRef.id).dropRight(4)).lastName
      }.getOrElse("")
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

  sealed trait BaseR {
    def id: Int
  }

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


  def createTableSource(split: List[String]): TableSource = {
    if (split.size == 5) {
      CyberTableRef(CyberRef(split(2).toInt))
    } else {
      DissTableRef(DissRef(split(2).toInt), split(3).toInt)
    }
  }

  lazy val tableCnts: scala.collection.mutable.Map[(TableSource, TableSource), Int] = {
    val res = scala.collection.mutable.Map[(TableSource, TableSource), Int]().withDefaultValue(0)

    GroupIterator[String, String, List[String]](scala.io.Source.fromFile("dctu.table").getLines(), line => {
      val split = line.split("\t")

      (split.take(2).mkString("\t"), List(line))
    },
      {
        (a, b) => a ++ b
      }
    ).foreach { case (_, list) =>
      for (p <- list.combinations(2).toList) {
        val fst = createTableSource(p.head.split("\t").toList)
        val snd = createTableSource(p.last.split("\t").toList)

        res((fst, snd)) = res((fst, snd)) + 1
        res((snd, fst)) = res((snd, fst)) + 1
      }
    }

    res
  }

  lazy val tableMin = tableCnts.filter(_._2 >= 10).filter { x =>
    Try {
      BaseRef1.year(getRef(x._1._1)) + BaseRef1.year(getRef(x._1._2))
    }.isSuccess
  }

  lazy val tableKeys: Vector[(TableSource, TableSource)] = tableMin.keySet.toVector

  lazy val tableMap: Map[BaseR, Vector[(TableSource, TableSource)]] = tableKeys.groupBy(x => getRef(x._1))

  lazy val tableMap1: Map[BaseR, Map[BaseR, Vector[(TableSource, TableSource)]]] = tableMap.mapValues { vec =>
    vec.groupBy(x => getRef(x._2))
  }

  lazy val tableByYear: Vector[(BaseR, Map[BaseR, Vector[(TableSource, TableSource)]])] = tableMap1.toVector.filter(x => Try {
    BaseRef1.year(x._1)
  }.isSuccess).sortBy(x => BaseRef1.year(x._1))

  def checkCyber(baseR: BaseR) = {
    baseR match {
      case CyberRef(q) =>
        if (!cyberTitle.contains(cyberMetaRev(q).dropRight(4))) {
          val link = s"https://cyberleninka.ru/article/n/${cyberMetaRev(q).dropRight(4)}"
          FileUtils.appendLine("cdown.txt", link);
        }
      case _ => ()
    }
  }

  def writeDiss(baseR: BaseR) = {
    baseR match {
      case DissRef(id) =>
        FileUtils.appendLine("dissD.txt", baseR.toString);
      case _ => ()
    }
  }


  //  //DocId, PageNum
  //  lazy val tableSet = {
  //    val res = scala.collection.mutable.Set[(Int, Int)]()
  //
  //    scala.io.Source.fromFile("ff.tt").getLines().foreach { line =>
  //      val split = line.split("\t")
  //      res.add((split(2).toInt, split(3).toInt))
  //    }
  //
  //    res
  //  }

  //DocId, PageNum
  lazy val tableSet: collection.Set[(Int, Int)] = {
    val res = scala.collection.mutable.Map[(Int, Int), Int]().withDefaultValue(0)

    scala.io.Source.fromFile("tt.tt").getLines().foreach { line =>
      val split = line.split("\t")
      res((split(2).toInt, split(3).toInt)) = res((split(2).toInt, split(3).toInt)) + 1
    }

    res.filter(_._2 >= 10).keySet
  }

  //doc1, page1, doc2, page2
  lazy val tablePageMap: mutable.Map[(Int, Int, Int, Int), Int] = {
    val res = scala.collection.mutable.Map[(Int, Int, Int, Int), Int]().withDefaultValue(0)
    GroupIterator[String, String, List[String]](scala.io.Source.fromFile("tt.tt").getLines(), line => {
      val split = line.split("\t")

      (split.take(2).mkString("\t"), List(line))
    },
      {
        (a, b) => a ++ b
      }
    ).foreach { case (_, list) =>
      val pages = list.map(x => x.split("\t").last.toInt)
      val ids = list.map(x => x.split("\t").dropRight(1).last.toInt)
      ids.zip(pages).combinations(2).foreach {
        ll =>

          val ii = ll.head
          val jj = ll.last
          if (tableSet.contains(ii._1, ii._2) && tableSet.contains(jj._1, jj._2)) {
            res((ii._1, ii._2, jj._1, jj._2)) = res((ii._1, ii._2, jj._1, jj._2)) + 1
            res((jj._1, jj._2, ii._1, ii._2)) = res((jj._1, jj._2, ii._1, ii._2)) + 1
          }
      }
    }

    res
  }

  lazy val resTable: mutable.Map[(Int, Int, Int, Int), Int] = tablePageMap.filter { case (_, v) => v >= 10 }.filter { case ((a, _, c, _), _) => a != c }

  lazy val resTableByPage: Map[(Int, Int), List[((Int, Int), Int)]] = resTable.toList.groupBy(x => (x._1._1, x._1._3)).mapValues(x => x.map(z => ((z._1._2, z._1._4), z._2)))
  lazy val resTableCount: Map[(Int, Int), Int] = resTableByPage.mapValues(x => x.map(_._2).sum)

  def read(fileName: String): String = {
    import java.io._
    val br = new BufferedReader(new InputStreamReader(
      new FileInputStream(fileName), "UTF-8"))
    val res = br.lines().toArray.toList.map(_.toString).mkString(" ")
    br.close()
    res
  }

  def readWoBib(fileName: String, dropFirst: Int = 5): String = {
    val bibList = List[String](
      "СПИСОК ЛИТЕРАТУРЫ", "СПИСОК ИСПОЛЬЗОВАННОЙ", "СПИСОК ИСПОЛЬЗУЕМОЙ", "СПИСОКЛИТЕРАТУРЫ", "УКАЗАТЕЛЬ ЛИТЕРАТУРЫ",
      "СПИСОК ЦИТИРУЕМОЙ", "ИСПОЛЬЗОВАННАЯ ЛИТЕРАТУРА", "БИБЛИОГРАФИЧЕСКИЙ СПИСОК", "СПИСОК ИСПОЛЬЗОВАННЫХ",
      "БИБЛИОГРАФИЧЕСКИЙ", "БИБЛИОГРАФИЯ", "СПИСОК ИСТОЧНИКОВ", "ЛИТЕРАТУРА")

    val endList = List[String]("ВЫВОДЫ", "ЗАКЛЮЧЕНИЕ")

    val r = read(fileName)

    val a = r.split(12.toChar)

    val zip: Array[(String, Int)] = a.zipWithIndex
    val rev = zip.reverse

    val bib: Option[Int] = rev.take((rev.size * 3 / 4).toInt).find { case (str, _) =>
      val list = str.take(100).toUpperCase.filter(x => x.isLetter || x == ' ').split(" ").filter(_.nonEmpty)
      val head = list.mkString(" ")
      bibList.exists(prefix => head.startsWith(prefix))
    }.map(_._2)

    val end: Option[Int] = rev.take((rev.size * 3 / 4).toInt).find { case (str, _) =>
      val list = str.take(100).toUpperCase.filter(x => x.isLetter || x == ' ').split(" ").filter(_.nonEmpty)
      val head = list.mkString(" ")
      endList.exists(prefix => head.startsWith(prefix))
    }.map(_._2)

    val endPos = (bib, end) match {
      case (Some(b), Some(e)) => scala.math.min(b, e)
      case (None, Some(e)) => e
      case (Some(b), None) => b
      case _ => a.size - 10
    }

    zip.slice(dropFirst, endPos).map(_._1).mkString(12.toChar.toString)
  }

  def extractPageContext(ref: DissRef, page: Int): List[String] = {
    import scala.util.Try
    Try {

      val a: Array[String] = read(fullPathDiss(ref)).split(12.toChar)

      val p = a(page).toUpperCase

      val sp: List[String] = p.filter(x => x.isLetterOrDigit || x == ' ').split(" ").filter(_.nonEmpty).mkString(" ").split("ТАБЛИЦ").toList

      val res: List[String] = (1 to sp.size - 1).map { idx =>
        s"${sp.take(idx).mkString("ТАБЛИЦ").takeRight(1)}ТАБЛИЦ${sp.drop(idx).mkString("ТАБЛИЦ").take(150)}"

      }.toList

      if (res.isEmpty) {
        List("+- СОСЕДНЯЯ СТРАНИЦА, НЕ СОДЕРЖИТ ПОДСТРОКИ ТАБЛИЦ")
      } else {
        res
      }
    }.getOrElse(List[String]("*********"))
  }

  def fullPathDiss(diss: DissRef, pathFunc: (String, String) => String = customPathFunc): String = {
    fullPath(s"${getDissMeta(diss.id).id}.txt", pathFunc)
  }

  def customPathFunc(dir: String, fileName: String): String = {
    s"d/data1/$dir/$fileName"
  }

  def fullPath(fileName: String, pathFunc: (String, String) => String = customPathFunc): String = {
    val dir = loc(fileName)
    pathFunc(dir, fileName)
  }

  def extract(fst: DissRef, snd: DissRef, fstPage: Int, sndPage: Int): List[String] = {
    val fstList: List[String] = extractPageContext(fst, fstPage).map(x => s"${fstPage + 1}: ${x}")
    val sndList: List[String] = extractPageContext(snd, sndPage).map(x => s"${sndPage + 1}: ${x}")

    val add = scala.math.max(fstList.size, sndList.size) - scala.math.min(fstList.size, sndList.size)

    val (f: List[String], s: List[String]) = (fstList.size, sndList.size) match {
      case (a, b) if a < b => (fstList ++ List.fill[String](add)(""), sndList)
      case _ => (fstList, sndList ++ List.fill[String](add)(""))
    }

    f.zip(s).map { case (a, b) => s"\t\t\t\t\t${a}\t${b}"
    }
  }

  lazy val cyberMetaPath: Map[Int, Int] = scala.io.Source.fromFile("down/meta.c").getLines().map { line =>
    val split = line.split("\t")
    (split(0).toInt, split(1).toInt)
  }.toMap

  def getPath(baseR: BaseR): String = {
    baseR match {
      case CyberRef(id) =>
        s"c/${cyberMetaPath(id)}/${cyberMetaRev(id)}"

      case DissRef(id) =>
        val idd = getDissMeta(id).id
        s"dissTable/${idd}.txt"
    }
  }

  def normalizeTableStr(str: String): String = {
    str.filter(ch => ch == ' ' || (ch >= 'а' && ch <= 'Я') || (ch >= '\u0410' && ch <= '\u044F') || ch.isDigit).toUpperCase.split(" ").filter(_.nonEmpty).mkString(" ")
  }

  def getContextFromCyber(ref: CyberTableRef): List[String] = {
    val text = normalizeTableStr(scala.io.Source.fromFile(getPath(ref.diss), "UTF-8").getLines().mkString("\n"))
    val sp = text.split("ТАБЛИЦ").toList
    (1 to sp.size - 1).map { idx =>
      s"${sp.take(idx).mkString("ТАБЛИЦ").takeRight(1)}ТАБЛИЦ${sp.drop(idx).mkString("ТАБЛИЦ").take(150)}"
    }.toList
  }

  def getContextFromDiss(diss: DissTableRef): List[String] = {
    val a: String = {
      import java.io._
      val br = new BufferedReader(new InputStreamReader(
        new FileInputStream(getPath(diss.diss)), "UTF-8"))
      br.lines().toArray.toList.map(_.toString).mkString("")
    }

    val pages = a.split(12.toChar)

    val text = normalizeTableStr(pages(diss.page))


    val sp = text.split("ТАБЛИЦ").toList
    (1 to sp.size - 1).map { idx =>
      s"${sp.take(idx).mkString("ТАБЛИЦ").takeRight(1)}ТАБЛИЦ${sp.drop(idx).mkString("ТАБЛИЦ").take(150)}"
    }.toList
  }


  def getContext(src: TableSource): List[String] = {
    src match {
      case x@CyberTableRef(_) => getContextFromCyber(x)
      case x@DissTableRef(_, page) => List(s"page: $page") ++ getContextFromDiss(x)
    }
  }


  def getSource(tableSource: TableSource): BaseR = {
    tableSource match {
      case CyberTableRef(art) => art
      case DissTableRef(diss, page) => diss
    }
  }


  case class TableRecord(mainRef: List[String], mainContext: List[String], otherContext: List[TableRecordItem])

  case class TableRecordItem(otherRef: List[String], context: List[String])

  lazy val workSizes: Map[Int, String] = scala.io.Source.fromFile("size.size").getLines.map { line =>
    val split = line.split("\t")
    split(0).toInt -> split(1)
  }.toMap

  lazy val workSizesSI: Map[Int, Int] = workSizes.mapValues(_.split("-").last.toInt + 1)

  lazy val ngRealMap: mutable.Map[(BaseR, BaseR), Int] = {
    val res = scala.collection.mutable.Map[(BaseR, BaseR), Int]().withDefaultValue(0)

    scala.io.Source.fromFile("ngMap7").getLines.foreach { line =>
      val split = line.split("\t").map(_.toInt)
      if (split.last >= 7)
        res((fromId(split.head), fromId(split(1)))) = split.last
    }
    res
  }

  lazy val ngGrouped: Map[BaseR, Vector[BaseR]] = ngRealMap.toVector.groupBy(_._1._1).mapValues(_.map(x => x._1._2)).withDefaultValue(Vector.empty)

  lazy val ngVector: Vector[(BaseR, Vector[BaseR])] = ngGrouped.toVector

  def matchWord(fst: String, snd: String): Boolean = {
    val minLen = scala.math.min(fst.length, snd.length)
    (fst, snd, fst.length, snd.length) match {
      case (f, s, fl, sl) if f == s => true
      case (f, s, fl, sl) if scala.math.abs(sl - fl) <= 2 && fl >= 6 && f.take(fl - 2) == s.take(fl - 2) => true
      case (f, s, fl, sl) if scala.math.abs(sl - fl) <= 2 && fl == 5 && f.take(fl - 1) == s.take(fl - 1) => true
      case (f, s, fl, sl) if scala.math.abs(sl - fl) <= 2 && fl == 4 && f.take(minLen) == s.take(minLen) => true
      case (f, s, fl, sl) if scala.math.abs(sl - fl) <= 1 && fl == 3 && f.take(2) == s.take(2) => true
      case _ => false
    }
  }

  type Res = Int

  def prefixMatch(phrase: List[String], text: List[String], offset: Int): List[Res] = {
    (phrase, text) match {
      case (Nil, _) => List(offset)
      case (a :: tail, b :: tail1) if matchWord(a, b) => prefixMatch(tail, tail1, offset)
      case _ => Nil

    }
  }

  @tailrec
  def matchList(phrase: List[String], text: List[String], acc: List[Res], offset: Int): List[Res] = {
    (phrase, text) match {
      case (_, Nil) => acc
      case (a :: t1, b :: t2) if matchWord(a, b) => matchList(phrase, t2, prefixMatch(t1, t2, offset) ++ acc, offset + 1)
      case (_, _ :: tail1) => matchList(phrase, tail1, acc, offset + 1)
    }
  }

  def matchText(listSpit: List[List[String]], values: List[String]): List[(String, String)] = {
    listSpit.zipWithIndex.flatMap { case (phrase, id) =>
      matchList(phrase, values, Nil, 0).map { offset =>
        val str = values.slice(offset, offset + phrase.size).mkString(" ")
        //        s"${phrase.mkString(" ")} -> $str"
        phrase.mkString(" ") -> str
      }
    }
  }

  def prettyPrint(values: List[(String, String)]): String = {
    if (values.isEmpty) {
      ""
    } else {

      val map = values.groupBy(_._1).mapValues(_.map(_._2)).mapValues { list =>
        list.groupBy(identity).mapValues(x => x.size).toList.toString
      }

      s"${values.size}\t${map.size}\t${map.toList.toString}"

    }
  }

  def req(values: String, url: String = "http://0.0.0.0:9875/pred"): List[(Int, String)] = {
    val r = requests.post(url, data = ujson.Obj("data" -> values).render(),
      headers = Map(
        "Content-Type" -> "application/json"
      )
    )

    val t = r.text("UTF-8")

    ujson.read(t).arr.map { v =>
      (v("idx").num.toInt -> v("word").str)
    }.toList
  }

  import scala.math.min

  def editDist[A](a: Iterable[A], b: Iterable[A]): Int =
    a.foldLeft((0 to b.size).toList) { (prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
        case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
      }
    }.last

  lazy val generalWords: Set[String] = {
    scala.io.Source.fromFile("general.txt", "UTF-8").getLines().map(_.split(" ").last.toUpperCase()).toSet
  }

  lazy val medicineCache: Map[Int, List[(Int, String)]] = {
    scala.io.Source.fromFile("medcache.txt", "UTF-8").getLines().map { line =>
      val split = line.split("\t")
      val id = split.head.toInt

      val other: List[(Int, String)] = split.last.split("/").map { item =>
        val split = item.split("\\|")
        split.head.toInt -> split.last
      }.toList.filter(x => !generalWords.contains(x._2))

      id -> other
    }.toMap.withDefaultValue(Nil)
  }

  lazy val textGraph: mutable.Map[(String, String), Res] = {
    val map = scala.collection.mutable.Map[(String, String), Int]().withDefaultValue(0)
    scala.io.Source.fromFile("graph.g.s").getLines.foreach { line =>
      val split = line.split("\t")
      if (split.head.startsWith("D") && split(2).startsWith("D")) {
        map(split.head.drop(1) -> split(2).drop(1)) = split(3).toInt
      }
    }

    map
  }
}

object UaMeta {
  case class UaMeta(id: String, author: String, fullDate: String, title: String) {
    lazy val year: Int = fullDate.split("-").head.toInt

    lazy val spec: String = title.drop(title.indexOf("spec.. ") + 7).takeWhile(c => c == '.' || c.isDigit)
  }

  lazy val uaMetaVec: Vector[UaMeta] = {
    scala.io.Source.fromFile("metadata.csv", "UTF-8").getLines.map { line =>
      val split = line.split("\t")
      UaMeta(split(0), split(2), split(3), split(4))
    }.toVector
  }

  lazy val uaMeta: Map[String, UaMeta] = uaMetaVec.map(x => x.id -> x).toMap

  lazy val idUaMap: Map[Int, String] = uaMetaVec.zipWithIndex.map { case (m, id) => id -> m.id }.toMap
  lazy val idUaRevMap: Map[String, Int] = uaMetaVec.zipWithIndex.map { case (m, id) => m.id -> id }.toMap

  def getUaMetaById(id: Int): UaMeta = {
    uaMeta(idUaMap(id))
  }

  def getUaMetaByStringId(id: String): UaMeta = {
    uaMeta(id)
  }

  def extractUaFromPath(path: String) = {
    path.split("/").dropRight(1).last
  }

  lazy val autoRefSet = Set("ref", "реферат", "avto")

  def isAutoRefUA(path: String): Boolean = {
    val fileName = path.split("/").last.toLowerCase
    autoRefSet.exists(x => fileName.contains(x))
  }

  def convertNgUa(str: String): String = {
    str.filter(ch => ch == ' ' || (ch >= 'а' && ch <= 'Я') || (ch >= '\u0410' && ch <= '\u044F')).toUpperCase
  }
}