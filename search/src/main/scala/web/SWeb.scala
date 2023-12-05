package web

import ujson._

import scala.util.Using

case class Person(id: Int, firstName: String, lastName: String, birthDate: String) {
  def toJson = ujson.Obj(
    "id" -> id,
    "firstName" -> firstName,
    "lastName" -> lastName,
    "birthDate" -> birthDate
  )
}

object SWeb extends cask.MainRoutes {
  override def port: Int = 8082

  override def host: String = "0.0.0.0"

  @cask.get("/")
  def hello(): String = {
    "Hello World!"
  }

  lazy val people: Vector[Person] = {
    Using(scala.io.Source.fromFile("data/names.idx", "UTF-8")) { stream =>
      stream.getLines.map { line =>
        val split = line.split("\t")
        val id = split(0).toInt
        val r = split(1).split(",")
        val f = r(0).split(" ")
        Person(id, f(1), f(0), r(1))
      }.toVector
    }.getOrElse(Vector.empty)
  }

  @cask.staticFiles("/static/:path")
  def staticFileRoutes(path: String) = "static/" + path

  @cask.postJson("/findPerson")
  def findPerson(firstName: ujson.Value, lastName: ujson.Value, birthDate: ujson.Value): Value = {
    val find = people.iterator.filter(p => p.firstName.contains(firstName.str.toUpperCase) && p.lastName.contains(lastName.str.toUpperCase) && p.birthDate.contains(birthDate.str)).take(500).toVector
    find.map(_.toJson)
  }

  @cask.postJson("/find")
  def find(id: ujson.Value): Value = {
    val fileName = "data/final.s.txt"
    val lookFor = s"${id.str}\t"
    BinSearch.find(fileName, lookFor).toList.flatMap { offset =>
      BinSearch.read(fileName, offset, lookFor, 12000, 500).map { line =>
        val split = line.split("\t")
        val id = split(1).toInt
        val cnt = split(2).toInt

        people(id) -> cnt
      }
    }.toVector.map(x => ujson.Obj("person" -> x._1.toJson, "cnt" -> x._2))
  }

  initialize()
}
