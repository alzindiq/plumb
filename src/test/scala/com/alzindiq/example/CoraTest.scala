package com.alzindiq.example

import com.alzindiq.Plumber
import com.alzindiq.cluster.Clusterer
import com.alzindiq.indexer.FieldIndexer
import com.hp.hpl.plumb.bucketer.SimpleWordBucketer
import com.hp.hpl.plumb.indexer.InvertedIndex
import com.rockymadden.stringmetric.similarity.JaccardMetric
import org.scalatest.{Matchers, FlatSpec}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object CoraTest {
  val fileName = "cora-all-id.txt"
//val fileName = "cora-all-subset.txt"
  val coraPlumbers : Set[Plumber] = Source.fromFile(fileName).getLines().map(CoraRecord.apply(_,4)).
    filter(!_.isEmpty).map(_.get).toSet

  val trimmingFunction = (w:Any) => w.toString.replaceAll("[^a-zA-Z0-9 ]", "").trim

  val indexName1 = "authorsIndex"
  val indexName2 = "titleIndex"


  val keyExtractor1 = (p:Plumber) => p.asInstanceOf[CoraRecord].getAuthorNGrams().map(_.map(CoraTest.trimmingFunction.apply(_))).toList
  val keyExtractor2 = (p:Plumber) => p.asInstanceOf[CoraRecord].getTitleNGrams().map(_.map(CoraTest.trimmingFunction.apply(_))).toList
  val stopValues : Option[List[Any]] = Some(List("a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
    "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but",
    "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't",
    "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
    "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
    "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "learning", "let's", "me",
    "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once",
    "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same",
    "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those",
    "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll",
    "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's",
    "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't",
    "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"))

  def similarityFunction (threshold : Double) = (p : Plumber, p2 : Plumber) => {
    val coraP = p.asInstanceOf[CoraRecord]
    val coraP2= p2.asInstanceOf[CoraRecord]

    val jaccard = 0.8 *jaccardSimilarity(coraP.getTitleNGrams().flatten,coraP2.getTitleNGrams().flatten)
                + 0.2 *jaccardSimilarity(coraP.getAuthorNGrams().flatten,coraP2.getAuthorNGrams().flatten)

    if(jaccard >= threshold) jaccard
    else 0d
  }

  def jaccardSimilarity (s1 : Set[String], s2 : Set[String]) : Double = {
    if((s1.size == 0 ) || (s2.size ==0)){
      0d
    } else {
      s1.intersect(s2).size  / s1.union(s2).size
    }
  }
}

class CoraTest extends FlatSpec with Matchers {

  "Cora parser" should "create right number of records" in {
    val t1 = System.currentTimeMillis
    CoraTest.coraPlumbers.size shouldBe 1878
    val t2 = System.currentTimeMillis
    println("Test finished in " + (t2 - t1) + " msecs")
  }

  "Bucketer for cora" should "result in decently separated buckets" in {

    val indexer = new FieldIndexer(CoraTest.stopValues,CoraTest.trimmingFunction)
    val tMinus1 = System.currentTimeMillis
    indexer.createIndices(Map (CoraTest.indexName1 -> CoraTest.keyExtractor1, CoraTest.indexName2 -> CoraTest.keyExtractor2),CoraTest.coraPlumbers)
    val t0 = System.currentTimeMillis
    println("Indexing finished in " + (t0 - tMinus1) + " msecs")

    val bucketiser  : (Set[Plumber]) => Map [Any,Set[Plumber]]= (in : Set[Plumber]) => {
      val outMap = mutable.Map.empty[Any,mutable.Set[Plumber]]
      var i = -1
      in.foreach( p => {
        val cora=  p.asInstanceOf[CoraRecord]
        val sameTwoAuth = cora.getAuthorNGrams()
        val sameTwoTitle = cora.getTitleNGrams()
        sameTwoAuth.map(auth => {
          val sameAuth = indexer.indices.get(CoraTest.indexName1).get.getOrElse(auth, ListBuffer.empty).toSet
          sameTwoTitle.map(title => {
            val sameTitle =  indexer.indices.get(CoraTest.indexName2).get.getOrElse(title, ListBuffer.empty).toSet
            var bucket = outMap.getOrElse(auth ++ title, mutable.Set.empty)
            outMap.put(auth ++ title, bucket.union(sameTitle.union(sameAuth)))
          })
        })
      })
      val out = Map(outMap.toList : _ *) // make the map immutable
      out.map(p => (p._1,Set(p._2.toList : _ *))).filter(p => p._2.size >1) // make the sets immutable too
    }


    val t1 = System.currentTimeMillis
    val buckets = bucketiser.apply(CoraTest.coraPlumbers)
    val t2 = System.currentTimeMillis
    println("Bucketing finished in " + (t2 - t1) + " msecs. Size "+buckets.size)
    //buckets.foreach(p => println(p._1, p._2.map(_.getId.get.toString)))
    val t3 = System.currentTimeMillis
    val coallesced = buckets.values.toList.distinct // remove duplicates
    val t4 = System.currentTimeMillis
    println("Coallescing finished in " + (t4 - t2) + " msecs. Size "+coallesced.size)
    //println(coallesced.map(s => s.map( p => p.getId.get.toString)))
    var i = -1
    val duplicateFreeMap : Map[List[Any],Set[Plumber]] = coallesced.map(p=>{
      i = i+1
      List(i) -> p
    }).toMap

    val quickie = duplicateFreeMap.get(List(273)).get
    val testMap : Map[List[Any],Set[Plumber]] = Map(List(1)-> quickie)

    val clusterer = new Clusterer(CoraTest.similarityFunction(0.6))
    val t5 = System.currentTimeMillis
    //val results = clusterer.initClusters4AllBuckets(duplicateFreeMap)
    val results = clusterer.initClusters4AllBuckets(testMap)
    val t6 = System.currentTimeMillis
    println("Clustering finished in " + (t6 - t5) + " msecs")
    println(results.size)
    println(results.get(List(1)).size)
  }
}
