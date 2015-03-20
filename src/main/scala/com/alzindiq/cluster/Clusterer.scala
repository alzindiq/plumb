package com.alzindiq.cluster

import com.alzindiq.Plumber
import com.alzindiq.similarity.DeltaBruteSimilarityRanker

import spray.json.DefaultJsonProtocol
import util.control.Breaks._

import scala.collection.mutable

/**
 * Receives collections of sufficiently similar Plumber objects (in the same Set)
 * For each Set (bucket), it calculates a similarity graph and creates clusters of
 * similar objects.
 * The members of each clusters are methodically changed (following the merge -> split > move)
 * pattern to maximise a well-defined quality metric as described in
 * Gruenheid et al."Incremental Record Linkage". VLDB 2014.
 */
class Clusterer (pairSimilarityFunction : (Plumber,Plumber) => Double) {

  /* Warning: this non blocking Map works fine as long as each thread accesses its entry/bucket in the map.
           Highly sensitive to bugs letting threads access a wrong entry  */
  var bucket2plumber2ClusterMap = mutable.Map.empty[String, mutable.Map[Plumber,PlumbCluster]]
  var intraBucketSimilarities = mutable.Map.empty[String, DeltaBruteSimilarityRanker]


  def increment (buckets : Map[List[Any],List[Set[Plumber]]]) : Map[List[Any], List[PlumbLink]] = {
    buckets.toList.par.map(bucket => {
      val perBucketSimCalc = intraBucketSimilarities.get(bucket._1.toString).get
      val bucketName = bucket._1
      val changes = bucket._2.toList
      val newItems = changes(0).toSeq
      val updatedItems = changes(1).toSeq
      val deletedItems = changes(2).toSeq

      perBucketSimCalc.increment(newItems, updatedItems, deletedItems)

      deletedItems.foreach(p => {
        bucket2plumber2ClusterMap.getOrElse(bucket._1.toString,mutable.Map.empty).get(p).get.removePlumber(p)
        bucket2plumber2ClusterMap.getOrElse(bucket._1.toString,mutable.Map.empty).remove(p)
      })

      val queue : mutable.Queue[PlumbCluster] = mutable.Queue.empty
      // add individual cluster for each new record and replace existing updated ones
      newItems.foreach(p =>{
        val newCluster =  new PlumbCluster(p)
        bucket2plumber2ClusterMap.getOrElse(bucket._1.toString,mutable.Map.empty).put(p,newCluster)
        queue.enqueue(newCluster)
      })

      updatedItems.foreach(p => {
        val plumberCluster = bucket2plumber2ClusterMap.getOrElse(bucket._1.toString,mutable.Map.empty).filterKeys(_.getId.get.toString.equals(p.getId.get.toString)).head
        bucket2plumber2ClusterMap.getOrElse(bucket._1.toString,mutable.Map.empty).remove(plumberCluster._1)
        plumberCluster._2.removePlumber(plumberCluster._1)
        plumberCluster._2.addPlumber(p)
        bucket2plumber2ClusterMap.getOrElse(bucket._1.toString,mutable.Map.empty).put(p,plumberCluster._2)
        queue.enqueue(plumberCluster._2)
      })

      val toBeProcessed = (bucket._1, newItems.++(updatedItems).toSet)
      runClusteringAlgorithm(toBeProcessed, queue)
    }).toMap.seq
  }

  def initClusters4AllBuckets(buckets : Map[List[Any],Set[Plumber]]) : Map[List[Any], List[PlumbLink]] = buckets.toList.map(initClusters4Bucket(_)).toMap

  private def initClusters4Bucket(bucket: (List[Any], Set[Plumber])) : (List[Any], List[PlumbLink]) = {

    val plumbers = bucket._2.toList
    if(plumbers.size <2){
      println("Too small a bucket, skip to next")
      (bucket._1,List.empty)
    }else {
      bucket2plumber2ClusterMap.put(bucket._1.toString, mutable.Map.empty)

      val queue: mutable.Queue[PlumbCluster] = mutable.Queue.empty
      bucket._2.foreach(p => {
        val newCluster = new PlumbCluster(p)
        bucket2plumber2ClusterMap.get(bucket._1.toString).get.put(p, newCluster)
        queue.enqueue(newCluster)
      })
      // calculate initial similarities for Plumbers in the same bucket
      val similarityCalc = new DeltaBruteSimilarityRanker(Seq(pairSimilarityFunction), true, 0.0)
      similarityCalc.calculateSimilarity(plumbers, plumbers) // dedup mode
      intraBucketSimilarities.put(bucket._1.toString, similarityCalc)

      runClusteringAlgorithm(bucket, queue)
    }
  }

  def initSimSum(bucket: String, plumbers : Set[Plumber]): Map[Plumber, Double] = {
    val simSum = mutable.Map.empty[Plumber,Double]
    val plumberNeighboursPairs = plumbers.map(s => (s,getPlumbNeighbours(bucket,s, Set.empty)))
    val similarities = intraBucketSimilarities.get(bucket).get.getResults
    plumberNeighboursPairs.foreach(s => {
      val simM = similarities.getOrElse(s._1,Map.empty)
      simSum.put(s._1,s._2.map(simM.getOrElse(_, 0.0)).sum)
    })
    Map(simSum.toList : _ *)
  }

  private def runClusteringAlgorithm(bucket: (List[Any], Set[Plumber]), queue: mutable.Queue[PlumbCluster]): (List[Any], List[PlumbLink]) = {
    println("Starting with bucket "+bucket._1.toString+" size "+bucket._2.size)
    val tstart = System.currentTimeMillis
    val neighbouSimSumMap = initSimSum(bucket._1.toString, bucket._2)
    val similarities = intraBucketSimilarities.get(bucket._1.toString).get.getResults

    while (!queue.isEmpty) {
      var changed: Boolean = false
      val cluster = queue.dequeue()
      val testPenalty = PenaltyCalculator.calculatePenalty(bucket._1.toString,cluster.getClusterNodes, neighbouSimSumMap, similarities)
      val t1 = System.currentTimeMillis
      changed = merge(bucket._1.toString, cluster, queue, testPenalty, neighbouSimSumMap, similarities)
      val t2 = System.currentTimeMillis
      println("Merged finished in " + (t2 - t1) + " msecs. Success? "+changed)
//      println("********************************************")

      if (!changed) {
        //println(bucket._1.toString+" Not merged, try splitting ")
        val t3 = System.currentTimeMillis
        changed = split(bucket._1.toString, cluster, queue, testPenalty, neighbouSimSumMap, similarities)
        val t4 = System.currentTimeMillis
        println("Split finished in " + (t4 - t3) + " msecs. Success? "+changed)
      }

      if (!changed) {
//        println(bucket._1.toString+" Not split, try moving ")
        val t5 = System.currentTimeMillis
        changed = move(bucket._1.toString, cluster, queue, neighbouSimSumMap, similarities)
        val t6 = System.currentTimeMillis
        println("Move finished in " + (t6 - t5) + " msecs. Success? "+changed)
      }
//      if (!changed){
        println(bucket._1.toString+" Try next cluster in queue ")
//      }

    }
    val tend = System.currentTimeMillis
    println("Done with bucket "+bucket._1.toString+" in "+(tend-tstart))

    val formedClusters = bucket2plumber2ClusterMap.getOrElse(bucket._1.toString, mutable.Map.empty).values.toList.distinct

    val listOfLinks = formedClusters.map(_.getClusterNodes).map(transform2ListOfLinks(bucket._1.toString, _)).flatten
    (bucket._1, listOfLinks)
  }

  private def transform2ListOfLinks(bucket : String, plumbers: Set[Plumber]): List[PlumbLink] = {
    if(plumbers.size <= 1)
      List.empty
    else {
      plumbers.toList.combinations(2).map(list => PlumbLink(list(0).getId.get.toString, list(1).getId.get.toString, getSimilarity(bucket, list(0), list(1)), bucket + plumbers.map(_.getId.get.toString))).toList
    }
  }

  private def merge(bucket : String, testCluster : PlumbCluster, queue: mutable.Queue[PlumbCluster], testPenalty : Double, neighbouSimSumMap : Map[Plumber, Double], similarities : Map[Plumber,Map[Plumber,Double]]): Boolean ={
    val neighbourClusters = testCluster.getClusterNodes.map(getPlumbNeighbours(bucket,_, testCluster.getClusterNodes)).flatten.map(getCurrentCluster(bucket,_))

    // TODO: smart heuristic to pick a "maximum gain" merge-able cluster instead of the first one that gets an improvement
    var foundImprovement = false
    breakable {
      neighbourClusters.foreach(neighbour => {
        val newClusterMembers = testCluster.getClusterNodes.union(neighbour.getClusterNodes)
//        val t0 = System.currentTimeMillis
        val neighPenalty = PenaltyCalculator.calculatePenalty(bucket, neighbour.getClusterNodes,neighbouSimSumMap, similarities)
//        val t1 = System.currentTimeMillis
//        println("Neig "+(t1-t0))
        val separatePenalty = testPenalty + neighPenalty
//        val t2 = System.currentTimeMillis
        val mergedPenalty = PenaltyCalculator.calculatePenalty(bucket, newClusterMembers,neighbouSimSumMap, similarities)
//        val t3 = System.currentTimeMillis
//        println("Merged "+(t3-t2))

        if (mergedPenalty < separatePenalty) {
          foundImprovement = true
          val newCluster = new PlumbCluster(newClusterMembers)
          newClusterMembers.foreach(p => bucket2plumber2ClusterMap.getOrElse(bucket, mutable.Map.empty).put(p, newCluster))
          queue.enqueue(newCluster)
          queue.dequeueFirst(_.equals(neighbour))
          break
        }
      })
    }
    foundImprovement
  }
  private def split(bucket : String, testCluster : PlumbCluster, queue: mutable.Queue[PlumbCluster], originalPenalty : Double, neighbouSimSumMap : Map[Plumber, Double], similarities : Map[Plumber,Map[Plumber,Double]]): Boolean = {
    var foundImprovement = false

    if(testCluster.getClusterNodes.size >1){
      var newClusterPlumbers = mutable.Set.empty[Plumber]
      var newCohe = 0d
      var allCohe = PenaltyCalculator.cohesion(bucket,testCluster.getClusterNodes, similarities)

      testCluster.getClusterNodes.foreach(p => {
        newClusterPlumbers += (p)
        val remaining = testCluster.getClusterNodes.diff(newClusterPlumbers)
        val transCohe = newCohe +  newClusterPlumbers.filter(!_.equals(p)).map(getSimilarity(bucket,p,_)).sum

        val newClusterPenalty = PenaltyCalculator.calculatePenalty(bucket, Set(newClusterPlumbers.toList: _ *),neighbouSimSumMap, transCohe, similarities)
        val remainingPenalty = PenaltyCalculator.calculatePenalty(bucket, remaining,neighbouSimSumMap, allCohe - transCohe, similarities)
        if (newClusterPenalty + remainingPenalty >= originalPenalty) {
          newClusterPlumbers -= (p)
        } else {
          foundImprovement = true
          newCohe = transCohe
        }
      })

      if (foundImprovement) {
        val newClusterFromMovedPlumbers = new PlumbCluster(Set(newClusterPlumbers.toList: _ *))
        newClusterPlumbers.foreach(p => bucket2plumber2ClusterMap.getOrElse(bucket, mutable.Map.empty).put(p, newClusterFromMovedPlumbers))
        val remainingOfOriginal = testCluster.getClusterNodes.diff(newClusterPlumbers)
        val newClusterFromOriginal = new PlumbCluster(remainingOfOriginal)

        remainingOfOriginal.foreach(p => bucket2plumber2ClusterMap.getOrElse(bucket, mutable.Map.empty).put(p, newClusterFromOriginal))
        if(remainingOfOriginal.toList.size>0 && remainingOfOriginal.map(getPlumbNeighbours(bucket,_, testCluster.getClusterNodes)).flatten.map(getCurrentCluster(bucket,_)).size>0) queue.enqueue(newClusterFromOriginal)
        if(newClusterPlumbers.toList.size>0 && newClusterPlumbers.map(getPlumbNeighbours(bucket,_, testCluster.getClusterNodes)).flatten.map(getCurrentCluster(bucket,_)).size>0) queue.enqueue(newClusterFromMovedPlumbers)
      }
    }
    foundImprovement
  }
  private def move(bucket : String, testCluster : PlumbCluster, queue: mutable.Queue[PlumbCluster],neighbouSimSumMap : Map[Plumber, Double], similarities : Map[Plumber,Map[Plumber,Double]]): Boolean = {
    val neighbourClusters = testCluster.getClusterNodes.map(getPlumbNeighbours(bucket,_, testCluster.getClusterNodes)).flatten.map(getCurrentCluster(bucket,_))
    var foundImprovement = false

    var newTestClusterPlumbers = mutable.Set.empty[Plumber]
    testCluster.getClusterNodes.foreach(newTestClusterPlumbers+=(_))

    var plumClusMap=bucket2plumber2ClusterMap.getOrElse(bucket, mutable.Map.empty)
    val newTestPenalty = PenaltyCalculator.calculatePenalty(bucket,Set(testCluster.getClusterNodes.toList : _ *),neighbouSimSumMap,  similarities)

    neighbourClusters.foreach(neighbour => {
      var newTestClusterPlumbers = mutable.Set.empty[Plumber]
      testCluster.getClusterNodes.foreach(newTestClusterPlumbers+=(_))

      val newNeighPenalty = PenaltyCalculator.calculatePenalty(bucket,neighbour.getClusterNodes,neighbouSimSumMap, similarities)
      var originalPenalty = newTestPenalty + newNeighPenalty

      var neighbourChanged = false
      var newNeighbourPlumbers = mutable.Set.empty[Plumber]
      neighbour.getClusterNodes.foreach(newNeighbourPlumbers+=(_))

      testCluster.getClusterNodes.foreach( p => {
        breakable {
          // find neighbours in other cluster
          val otherClusterNeigh = getNeighboursFromOtherCluster(bucket, p, neighbour.getClusterNodes)

          if (otherClusterNeigh.size < 1) break;

          // move p to neighbour cluster
          newNeighbourPlumbers += (p)
          val remaining = newTestClusterPlumbers.toSet.diff(newNeighbourPlumbers)

          val newNeighPenalty = PenaltyCalculator.calculatePenalty(bucket, Set(newNeighbourPlumbers.toList: _ *), neighbouSimSumMap, similarities)
          val remainingPenalty = PenaltyCalculator.calculatePenalty(bucket, remaining, neighbouSimSumMap, similarities)

          if ((remainingPenalty + newNeighPenalty) < originalPenalty) {
            foundImprovement = true
            originalPenalty = remainingPenalty + newNeighPenalty
            neighbourChanged = true
            plumClusMap.put(p, neighbour)
          } else {
            newNeighbourPlumbers -= (p)
            // try move neighbours to test instead
            otherClusterNeigh.foreach(n => {
              newTestClusterPlumbers += (n)
              val remaining = newNeighbourPlumbers.toSet.diff(newTestClusterPlumbers)
              val newTestPenalty = PenaltyCalculator.calculatePenalty(bucket, Set(newTestClusterPlumbers.toList: _*),neighbouSimSumMap,  similarities)
              val remainingPenalty = PenaltyCalculator.calculatePenalty(bucket, remaining, neighbouSimSumMap, similarities)

              if ((remainingPenalty + newTestPenalty) < originalPenalty) {
                foundImprovement = true
                originalPenalty = remainingPenalty + newTestPenalty
                neighbourChanged = true
                plumClusMap.put(p, testCluster)
              } else {
                newTestClusterPlumbers -= (n)
              }
            })
          }
        }
      })

      if(neighbourChanged){
        neighbour.setClusterNodes(newNeighbourPlumbers.toList)
        queue.dequeueFirst(_.equals(neighbour))
        queue.enqueue(neighbour) // back to end of queue
      }
    })

    if(foundImprovement) {
      testCluster.setClusterNodes(newTestClusterPlumbers.toList)
      queue.enqueue(testCluster)
    }
    foundImprovement
  }

  private def getSimilarity(bucket : String, s: Plumber, d : Plumber) = intraBucketSimilarities.get(bucket).get.getResults.getOrElse(s,Map.empty).getOrElse(d, 0.0)

  private def getNeighbourClusters(bucket : String, p : Plumber, excludeSet : Set[Plumber] = Set.empty) : Set[PlumbCluster] = {
    val neighbourPlumbers = getPlumbNeighbours(bucket, p, excludeSet)
    neighbourPlumbers.map(getCurrentCluster(bucket,_))
  }

  def getNeighboursFromOtherCluster(bucket: String, p : Plumber, neighbours : Set[Plumber]) : Set[Plumber] = {
    val results = intraBucketSimilarities.get(bucket).get.getResults
    results.get(p).get.keySet.filter(neighbours.contains(_)).toSet
  }

  private def getPlumbNeighbours (bucket: String, p : Plumber, excludeSet : Set[Plumber]) : Set[Plumber] = {
    val results = intraBucketSimilarities.get(bucket).get.getResults
    if (results.get(p).isEmpty) {
      Set.empty
    } else {
      results.get(p).get.keySet.filter(!excludeSet.contains(_))
    }

  }

  private def getCurrentCluster (bucket: String, p : Plumber) : PlumbCluster =  bucket2plumber2ClusterMap.getOrElse(bucket,mutable.Map.empty).get(p).get

}

class PlumbCluster  {
  private var plumbers : mutable.Set[Plumber] = mutable.Set.empty

  def this(plumbs : Set[Plumber]) = {
    this()
    this.plumbers = mutable.Set(plumbs.toList : _*)
  }

  def this(p : Plumber) = {
    this()
    plumbers.+=(p)
  }
  
  def addPlumber (p : Plumber) = plumbers+=(p)

  def removePlumber (p : Plumber) = plumbers-=(p)

  def getClusterNodes : Set[Plumber] = collection.immutable.Set(plumbers.toList: _*)

  def setClusterNodes(list : List[Plumber]) = this.plumbers = mutable.Set(list : _*)

}

case class PlumbLink(source : String, dst : String, value : Double, group: String)

object PlumbLink extends DefaultJsonProtocol {
  implicit val linkFormat = jsonFormat4(PlumbLink.apply)
}
