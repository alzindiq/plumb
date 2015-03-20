package com.alzindiq.bucketer

import com.alzindiq.Plumber
import com.alzindiq.indexer.{InvertedIndex, FieldIndexer}
import com.alzindiq.similarity.AbstractSimilarityRanker
import Plumber
import com.hp.hpl.plumb.indexer.InvertedIndex
import org.scalatest.{Matchers, FlatSpec}

class InvertedIndexTest  extends FlatSpec with Matchers {
  "Blocker for numbers" should "create right blocks" in {
    val numToIndex = 3
    val seq1=(1 to numToIndex).map(i => Plumber(Map(AbstractSimilarityRanker.plumbId -> i, "names" -> ((i%2).toString+","+(i-1).toString)))).toSeq
    val seq2=(1 to numToIndex).map(i => Plumber(Map(AbstractSimilarityRanker.plumbId -> i, "names" -> ((i%3).toString+","+(i-1).toString)))).toSeq
    val indexName1 = "numberIndex"
    val indexName2 = "nameIndex"
    val keyExtractor1 = (p:Plumber) => List(p.getId.get.toString.toInt)
    val keyExtractor2 = (p:Plumber) => List(p.attributes.get("names").get.toString.split(",")).flatten

    val indexer = new FieldIndexer(None)
    val seq = seq1 ++ seq2
    indexer.createIndices(Map (indexName1 -> keyExtractor1, indexName2 -> keyExtractor2), (seq1 ++ seq2).toSet)

    val bucketer = new InvertedIndex(indexer.indices)
    // take groups of one from first index and groups of two from the second index
    val blockingFunction = (indexName2, 2)
    val t1 = System.currentTimeMillis
    val buckets = bucketer.createInvertedIndex(blockingFunction)
    var count = 0
    buckets.keySet.foreach( k => {
      if(k.equals(List("2", "1"))){
        val _2 =buckets.get(k).get.contains(Plumber(Map(AbstractSimilarityRanker.plumbId -> 2, "names" -> "2,1")))
        val _3 =buckets.get(k).get.contains(Plumber(Map(AbstractSimilarityRanker.plumbId -> 3, "names" -> "1,2")))
        (_2 && _3) shouldBe true
        count +=1
      }
      if(k.equals(List("1", "0"))){
        val _1 =buckets.get(k).get.contains(Plumber(Map(AbstractSimilarityRanker.plumbId -> 1, "names" -> "1,0")))
        val _2 =buckets.get(k).get.contains(Plumber(Map(AbstractSimilarityRanker.plumbId -> 2, "names" -> "0,1")))
        (_2 && _1) shouldBe true
        count +=1
      }
    })
    count shouldBe 2
    val t2 = System.currentTimeMillis
    println("Test finished in " + (t2 - t1) + " msecs")

  }
}
