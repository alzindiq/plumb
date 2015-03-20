package com.alzindiq.cluster

import com.alzindiq.Plumber
import com.alzindiq.similarity.AbstractSimilarityRanker
import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable

object ClustererTest {
  val one = TextPlumber(1,"one is one")
  val onePrime = TextPlumber(1,"this one is similar to other one")
  val two = TextPlumber(2, "one sismilar to one")
  val three = TextPlumber( 3, "one similar to one")
  val four = TextPlumber(4,  "one sismilar to one")
  val five = TextPlumber( 5, "oneis not the one")
  val six = TextPlumber(6,  "two is not the one")
  val seven = TextPlumber(7,  "another two is not the one")
  val eight = TextPlumber(8,  "this two is not that one")
  val nine = TextPlumber(9,  "two is two")
  val ten = TextPlumber(10, "three is here")
  val eleven = TextPlumber(11, "three is here to stay")
  val twelve = TextPlumber( 12,  "another three is here too")
  val thirteen = TextPlumber(13, "one two three nonsense")

  def similarityFunction (threshold : Double) = (p : Plumber, p2 : Plumber) => {
    val jaccard= JaccardMetric(2).compare(p.attributes.get("text").get.toString, p2.attributes.get("text").get.toString).get // use bigrams
    if(jaccard >= threshold) jaccard
    else 0d
  }
}

class ClustererTest  extends FlatSpec with Matchers {
  "clusterer " should "create right clusters without any delta" in{
    val bucket1 : Set[Plumber] = Set(ClustererTest.one, ClustererTest.two, ClustererTest.three, ClustererTest.four, ClustererTest.five)//, five, six, seven, eight, nine, ten, eleven, twelve, thirteen)
    val bucket2 : Set[Plumber]= Set(ClustererTest.six, ClustererTest.seven, ClustererTest.eight, ClustererTest.nine)
    val bucket3  : Set[Plumber] = Set(ClustererTest.ten, ClustererTest.eleven, ClustererTest.twelve, ClustererTest.thirteen)

    var buckets = mutable.Map.empty[List[Any],Set[Plumber]]
    buckets.put(List("one"), bucket1)
    var test = Map(buckets.toList : _*)

    val clusterer = new Clusterer(ClustererTest.similarityFunction(0.6))
    val results = clusterer.initClusters4AllBuckets(test)
    results.size shouldBe 1
    val res = results.head._2

    res.size shouldBe 3
    res.head.value should be > 0.7

    buckets.put(List("two"), bucket2)
    buckets.put(List("three"),bucket3)

    test = Map(buckets.toList : _*)
    val threeResults = clusterer.initClusters4AllBuckets(test)

    threeResults.get(List("two")).head.size shouldBe 3
    threeResults.get(List("three")).head.size shouldBe 1
  }

  "Clusterer" should "create increments without starting from stcratch" in {
    val bucket1 : Set[Plumber] = Set(ClustererTest.one, ClustererTest.three)
    val added : Set[Plumber] = Set(ClustererTest.two, ClustererTest.four, ClustererTest.five)
    val removed : Set[Plumber] = Set(ClustererTest.three)
    val modified : Set[Plumber] = Set(ClustererTest.onePrime)

    println("OnePrime to Two "+ClustererTest.similarityFunction(0.5).apply(ClustererTest.two,ClustererTest.onePrime))

    var buckets = mutable.Map.empty[List[Any],Set[Plumber]]
    buckets.put(List("one"), bucket1)
    var test = Map(buckets.toList : _*)

    val clusterer = new Clusterer(ClustererTest.similarityFunction(0.5))
    val results = clusterer.initClusters4AllBuckets(test)
    results.get(List("one")).head shouldBe List()

    val updates : List[Set[Plumber]] = List(added, modified, removed)
    val bucketName : List[Any] = List("one")
    val updateMap : Map[List[Any],List[Set[Plumber]]] = Map(bucketName -> updates)
    val incremented = clusterer.increment(updateMap)

    incremented.get(List("one")).size shouldBe 1
    incremented.get(List("one")).head.size shouldBe 1
    incremented.get(List("one")).head.head.source shouldBe "2"
    incremented.get(List("one")).head.head.dst shouldBe "4"
    incremented.get(List("one")).head.head.value shouldBe 1d
  }
}

object TextPlumber {
  def ngrams(n: Int, data : Array[String]) : Set[List[String]] = (2 to n).map(i => data.sliding(i).toStream).foldLeft(Stream.empty[Array[String]])((acc, res) => acc #:::res ).map(_.toList).toSet
  def apply(i : Int, text : String) = new TextPlumber(i, text)
}

class TextPlumber(i: Int, text : String) extends Plumber (Map(AbstractSimilarityRanker.plumbId -> i, "text" -> text, "ngramText" -> TextPlumber.ngrams(2,text.split(" "))))
