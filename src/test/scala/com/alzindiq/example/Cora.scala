package com.alzindiq.example

import com.alzindiq.Plumber
import com.alzindiq.similarity.AbstractSimilarityRanker

import scala.collection.mutable


object CoraRecord{
  val id = "id"
  val pid = "pid"
  val authors = "authors"
  val title = "title"
  val defaultNgrams = 2

  // [10, benford1993a] Steve Benford and Lennart E. Fahlen. - A spatial model of interaction in large virtual environments.
  def apply(line : String, n : Int = defaultNgrams) = {

    if(line.isEmpty)
      None
    else {
      var fields: collection.mutable.MutableList[String] = mutable.MutableList.empty
      val idAndPubIdOption = """\[.*\]""".r.findFirstIn(line)
      if (idAndPubIdOption.isEmpty) {
        fields += ""
        fields += ""
        val authAndTitle = line.split("-")
        if (authAndTitle.length != 2) {
          None
        } else {
          fields += authAndTitle(0).trim
          fields += authAndTitle(1).trim
          validateLength(n, fields)
        }
      } else {
        val idAndPub = idAndPubIdOption.get.replace("[", "").replace("]", "").split(",")
        fields += idAndPub(0).trim
        fields += idAndPub(1).trim
        val authAndTitle = line.substring(line.indexOf("]") + 1).split("-")
        fields += authAndTitle(0).trim
        fields += authAndTitle(1).trim
        validateLength(n, fields)
      }
    }
  }

  def validateLength(n: Int, fields: mutable.MutableList[String]): Option[CoraRecord] = {
    if (fields.length != 4) {
      None
    } else {
      Some(new CoraRecord(fields(0), fields(1), fields(2), fields(3), ngrams(n, fields(2).split(" ")), ngrams(n, fields(3).split(" "))))
    }
  }

  def ngrams(n: Int, data : Array[String]) : Set[Array[String]] = (2 to n).map(i => data.sliding(i).toStream).foldLeft(Stream.empty[Array[String]])((acc, res) => acc #:::res ).toSet
}

class CoraRecord(id : String, pubId: String, authors : String, title : String, authorNGram : Set[Array[String]], titleNGram : Set[Array[String]]) extends
Plumber(Map(AbstractSimilarityRanker.plumbId -> id, CoraRecord.pid -> pubId, CoraRecord.authors -> authors, CoraRecord.title -> title, CoraRecord.authors+"NGrams" -> authorNGram, CoraRecord.title+"NGrams" -> titleNGram)){

  def getAuthorNGrams() : Set[List[String]] = authorNGram.map(_.toList)

  def getTitleNGrams() : Set[List[String]] = titleNGram.map(_.toList)
}
