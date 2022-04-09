import $ivy.`com.github.melrief::purecsv:0.1.1`

import $ivy.`org.jsoup:jsoup:1.13.1`
import org.jsoup._
import org.jsoup.nodes.Element

import $ivy.`io.circe::circe-core:0.12.3`
import $ivy.`io.circe::circe-generic:0.12.3`
import $ivy.`io.circe::circe-parser:0.12.3`

import io.circe.generic.auto._, io.circe.syntax._

import io.circe.generic.auto._
import io.circe.syntax._
import io.circe._

import scala.collection.mutable.ArrayBuffer
import scala.annotation.tailrec

import $ivy.`com.lihaoyi::requests:0.7.0`
import $ivy.`com.lihaoyi::ujson:0.7.1`
