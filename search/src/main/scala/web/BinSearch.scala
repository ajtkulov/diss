package web

import java.io.RandomAccessFile

object BinSearch {
  def find(fileName: String, search: String, bufferSize: Int = 5000): Option[Long] = {
    val f = new RandomAccessFile(fileName, "r")

    var l: Long = 0
    var r: Long = (FileUtils.fileSize(fileName) - 1)

    while (r - l > 1) {
      val m: Long = (l + r) / 2
      val ar: Array[Byte] = Array.fill[Byte](bufferSize)(0)
      f.seek(m)
      f.read(ar)

      val index = ar.indexOf(10.toByte) max 0

      val str = new String(ar.drop(index + 1))

      if (str < search) {
        l = m
      } else {
        r = m
      }
    }

    val ar: Array[Byte] = Array.fill[Byte](bufferSize)(0)
    f.seek(r)
    f.read(ar)

    val index = ar.indexOf(10.toByte) max 0
    val res = new String(ar.drop(index + 1))

    if (res.startsWith(search)) {
      Some(r)
    } else {
      None
    }
  }

  def read(fileName: String, offset: Long, search: String, bufferSize: Int = 5000, limit: Int = 200): List[String] = {
    val f = new RandomAccessFile(fileName, "r")

    f.seek(offset)
    val ar: Array[Byte] = Array.fill[Byte](bufferSize)(0)
    f.read(ar)

    val index = ar.indexOf(10.toByte) max 0
    val last = ar.lastIndexOf(10.toByte)
    val res: String = new String(ar.slice(index, last), "UTF-8")

    val r = res.split("\n").filter(_.startsWith(search)).toList
    if (r.size > limit) {
      Nil
    } else {
      r
    }
  }

  def readBlob(fileName: String, offset: Long, bufferSize: Int = 5000): String = {
    val f = new RandomAccessFile(fileName, "r")

    f.seek(offset)
    val ar: Array[Byte] = Array.fill[Byte](bufferSize)(0)
    f.read(ar)

    val index = ar.indexOf(10.toByte) max 0
    val last = ar.lastIndexOf(10.toByte)
    new String(ar.slice(index, last), "UTF-8")
  }
}
