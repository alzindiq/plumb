package com.alzindiq.similarity

import com.alzindiq.Plumber
import Plumber
import org.scalatest.{FlatSpec, Matchers}


object StitcherTest{
  val leftCount = 5
  val rightCount = leftCount
  val left = (1 to leftCount).map(i => Plumber(Map(AbstractSimilarityRanker.plumbId -> i))).toSeq
  val right = (1 to rightCount).map(i => Plumber(Map(AbstractSimilarityRanker.plumbId -> i))).toSeq

  val intComparison = (b:Plumber,c:Plumber) => {
    if(b.getId.get.equals(c.getId.get)) 1.0
    else 0.0
  }

  val extraStitch = (b:Plumber,c:Plumber) => {
    if((b.getId.get.toString.toInt == 3 && c.getId.get.toString.toInt == 5 ) || (b.getId.get.toString.toInt == 5 && c.getId.get.toString.toInt == 3 )) 1.0
    else 0.0
  }

  val evenFilter = (p:Plumber) => p.getId.get.toString.toInt % 2 ==0
  val oddFilter = (p:Plumber) => p.getId.get.toString.toInt % 2 !=0
  val filterNum3 = (p:Plumber) => p.getId.get.toString.toInt!=3

}

class BruteForceSimilarityRankerTest extends FlatSpec with Matchers{
  "Unfiltered BruteForce" should "find a one to one mapping" in {
    val t1 = System.currentTimeMillis
    val noFilterStitcher : SimilarityRanker = new BruteForceSimilarityRanker(Seq(StitcherTest.intComparison), false) // dedup does not matter for a one off brute force stitcher
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)
    val result = noFilterStitcher.getResults
    result.values.map(seq => seq.values.foldLeft(0d)(_+_)).foldLeft(0d)(_ + _) shouldBe StitcherTest.leftCount
    result.values.flatMap(seq => seq.keySet).toList.sortWith((left,right) => left.getId.get.toString.toInt < right.getId.get.toString.toInt) shouldBe StitcherTest.left
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }

  "Even filtering BruteForce" should "find a one to one mapping" in {

    val t1 = System.currentTimeMillis
    val noFilterStitcher : SimilarityRanker = new BruteForceSimilarityRanker(Seq(StitcherTest.evenFilter),Seq(StitcherTest.evenFilter),Seq(StitcherTest.intComparison), false, 1.0)
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)
    val result = noFilterStitcher.getResults
    result.values.map(seq => seq.values.foldLeft(0d)(_+_)).foldLeft(0d)(_ + _) shouldBe StitcherTest.leftCount / 2
    result.values.flatMap(seq => seq.keySet).toList.sortWith((left,right) => left.getId.get.toString.toInt < right.getId.get.toString.toInt) shouldBe StitcherTest.left.filter(StitcherTest.evenFilter)
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }
}

class IncrementalSimilarityRankerTest extends FlatSpec with Matchers{

  val toBeAdded = Seq(Plumber(Map(AbstractSimilarityRanker.plumbId -> 6)))
  val toBeUpdated = Seq(Plumber(Map(AbstractSimilarityRanker.plumbId -> 5, "name" -> "replaced")))
  val toBeRemoved = Seq(Plumber(Map(AbstractSimilarityRanker.plumbId -> 2)),Plumber(Map(AbstractSimilarityRanker.plumbId -> 4)))

  "Unfiltered Incremental no dedup" should "update stitches" in {
    val t1 = System.currentTimeMillis
    val noFilterStitcher : IncrementalSimilarityRanker = new DeltaBruteSimilarityRanker(Seq(StitcherTest.intComparison), false)
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)

    noFilterStitcher.increment(toBeAdded,toBeUpdated,toBeRemoved)
    val result = noFilterStitcher.getResults
    // 6 should have no stitches in no dedup
    result.get(toBeAdded(0)) shouldBe None
    // 4 and 2 should have been deleted
    result.filter(r => toBeRemoved.contains(r)) shouldBe Map()
    // no dedup will render an entry for the original numer 5 and another for the modified 5
    result.filter(r => r._1.getId.get.toString.equals("5")).toList.size shouldBe 2
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }

  "Unfiltered Incremental with dedup" should "update stitches" in {
    val t1 = System.currentTimeMillis
    val noFilterStitcher : IncrementalSimilarityRanker = new DeltaBruteSimilarityRanker(Seq(StitcherTest.intComparison), true)
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)
    noFilterStitcher.increment(toBeAdded,toBeUpdated,toBeRemoved)
    val result = noFilterStitcher.getResults

    // 6 should have entry for stitches in no dedup
    result.get(toBeAdded(0)).get.keySet.toList(0) shouldBe toBeAdded(0)
    // 4 and 2 should have been deleted
    result.filter(r => toBeRemoved.contains(r)) shouldBe Map()
    // dedup will replace entry for the original numer 5 with the modified 5
    result.filter(r => r._1.getId.get.equals(toBeUpdated(0).getId.get) && r._1.attributes.get("name").get.toString.equals("replaced")).toList.size shouldBe 1
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }

  "Unfiltered Incremental no dedup with filter" should "update stitches" in {
    val t1 = System.currentTimeMillis
    val noFilterStitcher : IncrementalSimilarityRanker = new DeltaBruteSimilarityRanker(Seq(StitcherTest.filterNum3), Seq(StitcherTest.filterNum3),Seq(StitcherTest.intComparison), false, 1.0)
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)

    noFilterStitcher.increment(toBeAdded,toBeUpdated,toBeRemoved)
    val result = noFilterStitcher.getResults
    // 6 should have no stitches in no dedup
    result.get(toBeAdded(0)) shouldBe None
    // 4 and 2 should have been deleted
    result.filter(r => toBeRemoved.contains(r)) shouldBe Map()
    // no dedup will render an entry for the original numer 5 and another for the modified 5
    result.filter(r => r._1.getId.get.toString.equals("5")).toList.size shouldBe 2
    //
    result.filter(r => r._1.getId.get.toString.equals("3")).toList.size shouldBe 0
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }

  "Unfiltered Incremental with dedup with filter" should "update stitches" in {
    val t1 = System.currentTimeMillis
    val noFilterStitcher : IncrementalSimilarityRanker = new DeltaBruteSimilarityRanker(Seq(StitcherTest.oddFilter),Seq(StitcherTest.oddFilter),Seq(StitcherTest.intComparison), true, 1.0)
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)
    noFilterStitcher.increment(toBeAdded,toBeUpdated,Seq())
    val result = noFilterStitcher.getResults

    result.get(toBeAdded(0)) shouldBe None //filtered
    // 4 and 2 should not be there
    result.filter(r => toBeRemoved.contains(r)).toList.size shouldBe 0
    // dedup will replace entry for the original numer 5 with the modified 5
    result.filter(r => r._1.getId.get.equals(toBeUpdated(0).getId.get)).toList.size shouldBe 1
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }

  "Unfiltered Incremental with chained filters" should "return no results" in {
    val t1 = System.currentTimeMillis
    val noFilterStitcher : IncrementalSimilarityRanker = new DeltaBruteSimilarityRanker(Seq(StitcherTest.oddFilter,StitcherTest.evenFilter),Seq(StitcherTest.oddFilter),Seq(StitcherTest.intComparison), true, 1.0)
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)
    noFilterStitcher.increment(toBeAdded,toBeUpdated,Seq())
    val result = noFilterStitcher.getResults
    result.size shouldBe 0
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }


  "Unfiltered Incremental with dedup" should "should clean residual stitches" in {
    val t1 = System.currentTimeMillis
    val noFilterStitcher : IncrementalSimilarityRanker = new DeltaBruteSimilarityRanker(Seq(StitcherTest.intComparison, StitcherTest.extraStitch), true)
    noFilterStitcher.calculateSimilarity(StitcherTest.left,StitcherTest.right)
    val result = noFilterStitcher.getResults
    result.get(Plumber(Map(AbstractSimilarityRanker.plumbId -> 3))).get.keySet.map(p => p.getId.get.toString).toList.size shouldBe 2
    noFilterStitcher.increment(toBeAdded,Seq.empty,Seq(Plumber(Map(AbstractSimilarityRanker.plumbId -> 3))))
    val result2 = noFilterStitcher.getResults
    result2.get(Plumber(Map(AbstractSimilarityRanker.plumbId -> 5))).get.keySet.map(p => p.getId.get.toString).toList.size shouldBe 1
    val t2 = System.currentTimeMillis
    println("Test finished in "+(t2 - t1) + " msecs")
  }
}



//class EndpointTests extends Suites(new BruteForceStitcherTest, new Bar) with BeforeAndAfterAll {
//
//  override def beforeAll() {
//    println("Before!")  // start up your web server or whatever
//  }
//
//  override def afterAll() {
//    println("After!")  // shut down the web server
//  }
//}
