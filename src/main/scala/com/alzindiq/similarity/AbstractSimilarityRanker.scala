package com.alzindiq.similarity

import java.io.FileInputStream
import java.util.Properties
import com.alzindiq.Plumber
import Plumber
import scala.collection.JavaConverters._
import scala.collection.mutable

object AbstractSimilarityRanker{

  /**
   * Primary key of the Table: all Maps should have one plumbId entry for Plumb to recognise
   * the primary key of the object in the underlying sys, i.e. it is NOT a Plumb assigned id
   */
  val plumbId = "com.hp.hpl.plumb.Id"

  def parseDouble(s: Option[String]) = try { s.get.toDouble } catch { case _ : Throwable=> 0d }
  def parseInt(s: Option[String]) = try { s.get.toInt } catch { case _ : Throwable=> 0 }
  def parseBoolean(s: Option[String]) = try { s.get.toBoolean } catch { case _ : Throwable=> false }
}

abstract class AbstractSimilarityRanker( bFilters : Seq[Plumber => Boolean], cFilters : Seq[Plumber => Boolean],
                                       stitchCheckers : Seq[(Plumber, Plumber) => Double], dedup : Boolean, acceptanceThreshold : Double = 1.0) extends SimilarityRanker {

  // awful variables to keep side effects
  var lastRunSimilarities = initSimilarityMap
  var allItems = mutable.Seq.empty[Plumber]

  def initSimilarityMap = mutable.Map.empty[Plumber, mutable.Map[Plumber, Double]]
  def execStitching(filteredB: Seq[Plumber], filteredC: Seq[Plumber]): Unit

  override def getResults = {
    val immutableSeq = lastRunSimilarities.map(m => m._1 -> Map(m._2.toSeq : _*))
    Map(immutableSeq.toSeq : _*)
  }

  override def calculateSimilarity(leftElements: Seq[Plumber], rightElements: Seq[Plumber]): Unit = {
    val filteredB = leftElements.filter(b => applyAllFilters(bFilters, b))
    if(!dedup) {
      val filteredC = rightElements.filter(c => applyAllFilters(cFilters, c))
      allItems = mutable.Seq(filteredC.toSeq: _*)
      execStitching(filteredB, filteredC)
    }else{
      allItems = mutable.Seq(filteredB.toSeq: _*)
      execStitching(filteredB, filteredB)
    }
  }

  protected def applyAllFilters(filters: Seq[Plumber => Boolean], t: Plumber): Boolean = {
    if (filters.size == 0) {
      return true
    }else {
      filters.map(filter => filter.apply(t)).foldLeft(true)(((t, z) => t && z))
    }
  }
  //  I will go to hell for this horrible side-effect!
  protected def updateStitchMap(filteredB: Seq[Plumber], filteredC: Seq[Plumber],
                                checker: (Plumber, Plumber) => Double): Unit = {
    for (b <- filteredB) {
      for (c <- filteredC) {
        var bSimilarities = lastRunSimilarities.get(b).getOrElse(mutable.Map.empty)
        var storedSimilarity = bSimilarities.get(c).getOrElse(0d)
        val similarity = checker.apply(b, c)
        if (similarity >= acceptanceThreshold) {
          updateResultMaps(b, c, similarity)
        }
      }
    }
  }

  protected def updateResultMaps(b: Plumber, c: Plumber, similarity: Double): Unit = {
    var bResultMap: mutable.Map[Plumber, Double] = lastRunSimilarities.get(b).getOrElse(mutable.Map.empty)
    bResultMap.put(c, similarity)
    lastRunSimilarities += (b -> bResultMap)
  }
}
