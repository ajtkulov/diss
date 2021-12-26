package web

object BinSearch {
  def find(fileName: String, search: String, bufferSize: Int = 5000): Option[Long] = {
    import java.io.RandomAccessFile

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
    println(res)

    if (res.startsWith(search)) {
      Some(r)
    } else {
      None
    }
  }
}
