package com.alzindiq.similarity

import com.alzindiq.Plumber
import Plumber


trait SimilarityRanker {
  /**
   * Finds relationships between the elements in the base collection and the ones in the candidate
   * collection.
   * @param leftElements
   * @param rightElements
   * @return
   */
  def calculateSimilarity(leftElements : Seq[Plumber], rightElements : Seq[Plumber]) : Unit

  def getResults : Map[Plumber, Map[Plumber, Double]]

}

trait IncrementalSimilarityRanker extends SimilarityRanker {

  def increment (newItems : Seq[Plumber], updatedItems : Seq[Plumber], deletedItems : Seq[Plumber])  :  Unit

}
