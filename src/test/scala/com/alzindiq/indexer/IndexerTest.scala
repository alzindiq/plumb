package com.alzindiq.indexer

import com.alzindiq.Plumber
import com.alzindiq.similarity.AbstractSimilarityRanker
import Plumber
import AbstractSimilarityRanker
import org.scalatest.{Matchers, FlatSpec}

class FieldIndexerTest extends FlatSpec with Matchers {
  "Number indexer" should "create right indices" in {
    val numToIndex = 5
    val seq1=(1 to numToIndex).map(i => Plumber(Map(AbstractSimilarityRanker.plumbId -> i, "name" -> i))).toSeq
    val seq2=(1 to numToIndex).map(i => Plumber(Map(AbstractSimilarityRanker.plumbId -> i, "name" -> -i))).toSeq
    val indexName1 = "numberIndex"
    val indexName2 = "-numberIndex"
    val keyExtractor1 = (p:Plumber) => List(p.getId.get.toString.toInt)
    val keyExtractor2 = (p:Plumber) => List(p.attributes.get("name").get.toString)
    val check = Plumber(Map(AbstractSimilarityRanker.plumbId -> 1, "name" -> 1))
    val indexer = new FieldIndexer(None)
    val seq = seq1 ++ seq2

    val t1 = System.currentTimeMillis
    indexer.createIndices(Map (indexName1 -> keyExtractor1, indexName2 -> keyExtractor2), (seq1 ++ seq2).toSet)
    indexer.indices.get("randomName") shouldBe None
    indexer.indices.get(indexName1).get.size shouldBe 5
    indexer.indices.get(indexName2).get.size shouldBe 10
    indexer.indices.get(indexName1).get.get(1).get.size shouldBe 2
    indexer.indices.get(indexName1).get.get(1).get.contains(check) shouldBe true
    indexer.indices.get(indexName2).get.get("1").get.size shouldBe 1
    indexer.indices.get(indexName2).get.get("1").get.contains(check) shouldBe true

    val t2 = System.currentTimeMillis
    println("Test finished in " + (t2 - t1) + " msecs")
  }

  "Word indexer" should "create right indices" in {
    val indexer = new FieldIndexer(None)
    val indexName = "wordIndex"
    val keyExtractor = (p:Plumber) => List(p.getId.get.toString)
    val stopWords = List("plumb")
    val toBeTrimmedNames = List( "tr+im.-$£")
    val trimmingFunction = (w:Any) => w.toString.replaceAll("[^a-zA-Z0-9]", "")

    val numToIndex = 5
    val seq=(1 to numToIndex).map(i =>{
      if(i==3) { // introduce stop word
        Plumber(Map(AbstractSimilarityRanker.plumbId -> stopWords(0)))
      }else {
        if (i == 5) { // trim
          Plumber(Map(AbstractSimilarityRanker.plumbId -> toBeTrimmedNames(0)))
        } else {
          Plumber(Map(AbstractSimilarityRanker.plumbId -> i.toString))
        }
      }
    }).toSeq

    val t1 = System.currentTimeMillis
    indexer.createIndices(Map (indexName -> keyExtractor), seq.toSet)
    println(indexer.indices)
    indexer.indices.get("randomName") shouldBe None
    indexer.indices.get(indexName).get.size shouldBe 5
    indexer.indices.get(indexName).get.get("3") shouldBe None
    indexer.indices.get(indexName).get.get(stopWords(0)).get.size shouldBe 1
    indexer.indices.get(indexName).get.get(toBeTrimmedNames(0)).get.size shouldBe 1
    indexer.indices.get(indexName).get.get("trim") shouldBe None

    val indexer2 = new FieldIndexer(Option(stopWords),trimmingFunction)
    indexer2.createIndices(Map(indexName -> keyExtractor), seq.toSet)
    println(indexer2.indices)
    indexer2.indices.get("randomName") shouldBe None
    indexer2.indices.get(indexName).get.size shouldBe 4
    indexer2.indices.get(indexName).get.get("3") shouldBe None
    indexer2.indices.get(indexName).get.get(toBeTrimmedNames(0)) shouldBe None
    indexer2.indices.get(indexName).get.get("trim").get.size shouldBe 1
    val t2 = System.currentTimeMillis
    println("Test finished in " + (t2 - t1) + " msecs")


    val indexer3 = new FieldIndexer(Option(stopWords))
    // trimming can be done more efficiently right when the word has been extracted,
    // it is provided in the constructor iof the indexer for convenience only (reusing pre-existing extractos from a library)
    val keyExtractorPlusTrimming = keyExtractor.andThen(_.map(trimmingFunction.apply(_)))
    indexer3.createIndices(Map(indexName -> keyExtractorPlusTrimming), seq.toSet)
    println(indexer3.indices)
    indexer3.indices.get("randomName") shouldBe None
    indexer3.indices.get(indexName).get.size shouldBe 4
    indexer3.indices.get(indexName).get.get("3") shouldBe None
    indexer3.indices.get(indexName).get.get(toBeTrimmedNames(0)) shouldBe None
    indexer3.indices.get(indexName).get.get("trim").get.size shouldBe 1
    val t3 = System.currentTimeMillis
    println("Test finished in " + (t3 - t2) + " msecs")
  }


}
