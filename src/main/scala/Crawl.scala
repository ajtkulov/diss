package trans

import org.jsoup.nodes._
import scala.util._

case class PageIndex(year: Int, month: Int, part: Int, index: Int) {
  def toPath = s"$year/$month/page_${part}_${index}.html"
}

case class Card(fullName: String, text: String, id: String, shortId: String, date: String) {
  def toText = Vector(shortId, id, fullName, date, text).map(_.replace("\t", "")).mkString("\t")
}

object Main1 extends App {
  implicit def bool2int(b: Boolean) = if (b) 1 else 0

  def save(pageIndex: PageIndex, content: String) = {
    FileUtils.write(s"./data/${pageIndex.toPath}", content)
  }

  def downloadPage(url: String): (String, Document) = {
    val raw = requests.get(url).data.toString()
    raw -> org.jsoup.Jsoup.parse(raw)
  }

  def amountOfPages(doc: Document): Int = {
    val res = doc.select("a.page-link[href^=/searchdb?tab]")

    val links: Array[Element] = res.toArray().map(_.asInstanceOf[Element])
    Try {
      links.flatMap { l =>
        Try {
          l.text().toInt
        }.toOption
      }.max
    }.getOrElse(0)
  }

  def page(year: Int, month: Int, fst: Int, pageNum: Int = 1): String = {
    val (s, f) = (fst, month) match {
      case (0, 12) => f"$year-${month}%02d-15" -> f"${year + 1}-01-01"
      case (0, _) => f"$year-${month}%02d-15" -> f"$year-${month + 1}%02d-01"
      case _ => f"$year-${month}%02d-01" -> f"$year-${month}%02d-15"
    }

    s"""https://nrat.ukrintei.ua/en/searchdb/?tab=big&typeSearch2=okd&dateFromSearch=$s&dateToSearch=$f&fullText=1&sortOrder=registration_date&sortDir=desc&pa=$pageNum"""
  }

  def pageDate(year: Int, month: Int, date: Int, pageNum: Int = 1): String = {
    val (s, f) = f"$year-${month}%02d-${date}%02d" -> f"$year-${month}%02d-${date}%02d"

    s"""https://nrat.ukrintei.ua/en/searchdb/?tab=big&typeSearch2=okd&dateFromSearch=$s&dateToSearch=$f&fullText=1&sortOrder=registration_date&sortDir=desc&pa=$pageNum"""
  }

  def getAll = {
    for {
      year <- 1994 to 1980 by -1
      month <- 1 to 12
      fst <- Vector(1, 0)
    } {
      val fstPageUrl = page(year, month, fst)
      println(fstPageUrl)
      val (raw, doc) = downloadPage(fstPageUrl)
      val size = amountOfPages(doc)
      val index = PageIndex(year, month, fst, 1)
      save(index, raw)
      println(index)

      (2 to size).foreach { idx =>
        val pageUrl = page(year, month, fst, idx)
        val (raw, _) = downloadPage(pageUrl)
        save(PageIndex(year, month, fst, idx), raw)
      }
    }

    for {
      year <- 2021 to 2021 by -1
      month <- 5 to 5
      date <- 24 to 31
    } {
      val fstPageUrl = pageDate(year, month, date)
      println(fstPageUrl)
      val (raw, doc) = downloadPage(fstPageUrl)
      val size = amountOfPages(doc)
      val index = PageIndex(year, month, date, 1)
      save(index, raw)
      println(index)

      (2 to size).foreach { idx =>
        val pageUrl = pageDate(year, month, date, idx)
        val (raw, _) = downloadPage(pageUrl)
        save(PageIndex(year, month, date, idx), raw)
      }
    }
  }


  val iter = FileUtils.traverse(".").filter(_.endsWith(".html")).iterator.flatMap { file =>
    val text = scala.io.Source.fromFile(file, "UTF-8").getLines.mkString("\n")
    val doc = org.jsoup.Jsoup.parse(text)
    extract(doc)
  }.map(_.toText)

  FileUtils.write("result", iter)

  def extract(doc: Document): Vector[Card] = {
    val idsDivs = doc.select("div.card_control.col-1.text-nowrap").toArray.map(_.asInstanceOf[Element])
    val ids = idsDivs.map { cc =>
      cc.selectFirst("a").attr("data")
    }.toVector

    val cards = doc.select("div.my-card-body").toArray.map(_.asInstanceOf[Element])

    val res = cards.map { card =>
      val names = card.select("span[name=name_full]").toArray.map(_.asInstanceOf[Element])
      assert(names.length == 1)
      val name = names.head.text

      val links = card.select("a").toArray.map(_.asInstanceOf[Element])
      val filterLinks = links.filter(link => link.attr("abs:href").contains("okd.ukrintei.ua/view/okd"))
      assert(filterLinks.length == 1)
      val shortId = filterLinks.head.text

      val text = card.text
      val date = text.drop(text.indexOf("presented. ") + 11).take(10)

      Card(name, card.text, "", shortId, date)
    }.toVector

    res.zip(ids).map { case (c, l) =>
      c.copy(id = l)
    }
  }
}

object Gen {
  val iter = scala.io.Source.fromFile("metadata.csv", "UTF-8").getLines.map { line =>
    val split = line.split("\t")
    val path = split(0).takeRight(3).toInt
    s"ua_diss ${split(1)} down/$path/${split(0)}.zip"
  }

  FileUtils.write("down.sh", iter)
}
