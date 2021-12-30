package web

case class Edge(source: String, dest: String, direction: String, weight: Int) {
  def normalize = {
    if (direction == "<-") {
      Edge(dest, source, "->", weight)
    } else {
      this
    }
  }

  def toEdgeString: String = {
    direction match {
      case "--" => s""""$source" -> "$dest" [label="$weight"];"""
      case _ => s""""$source" -> "$dest" [label="$weight" dir=both];"""
    }
  }
}

object Graph {
  val empty: String = "NOT_FOUND"
  val fileName = "data/graph.g.s"

  def toGraph(main: String, edges: List[Edge]): String = {
    s"""digraph G {  {    node [];    "$main" [shape = polygon];  }   ${edges.map(_.toEdgeString).mkString(" ")} }""".stripMargin
  }

  def find(ref: String): List[Edge] = {
    val lookFor = s"$ref\t"
    BinSearch.find(fileName, lookFor).toList.flatMap { offset =>
      BinSearch.read(fileName, offset, lookFor).map { line =>
        val split = line.split("\t")
        Edge(split(0), split(2), split(1), split(3).toInt * 10)
      }
    }
  }

  def graph(ref: String, width: Int): (String, List[Edge]) = {
    val edges = bfs(ref, width)

    (toGraph(ref, edges), edges)
  }

  def bfs(ref: String, width: Int): List[Edge] = {
    var layer = 1

    var set = Set(ref)
    var frontier = set

    val res: scala.collection.mutable.Set[Edge] = scala.collection.mutable.Set[Edge]()

    while ((layer <= width || frontier.isEmpty) && res.flatMap(x => List(x.source, x.dest)).toSet.size <= 100) {
      val edges = frontier.toList.flatMap(find)
      val newVertexies = edges.map(_.dest).toSet
      frontier = newVertexies -- set
      set = set ++ frontier
      res.addAll(edges)
      layer = layer + 1
    }

    res.toList.map(_.normalize).distinct
  }
}
