package com.alzindiq.indexer

import com.alzindiq.Plumber
import Plumber

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class InvertedIndex (indices: mutable.Map[String,mutable.Map[Any,ListBuffer[Plumber]]]) {
  /**
   *
   * @param indexingFunction Map of indices and number of identical entities in the index;
   *                         Assumes an implicit AND within and across indices
   * @return list of sets of Plumber elements representing entities in the same bucket.
   *         One set per key in the input map
   */
  def createInvertedIndex (indexingFunction : (String,Int) = ("default",0)) : Map[List[Any],Set[Plumber]] = {
    val indexOpt = indices.filter(indx => indexingFunction._1.equals(indx._1)).headOption

    if(!indexOpt.isEmpty) {
      // TODO: Need to paralellise this calc of combinations for massive dictionaries
      val buckets: List[List[Any]] = indexOpt.get._2.keySet.subsets(indexingFunction._2).map(_.toList).toList

      findIntersections(buckets, indexOpt.get._2)
    }else{
      Map.empty
    }
  }

  private def findIntersections(combinationLists: List[List[Any]], plumbers  : mutable.Map[Any,ListBuffer[Plumber]]): Map[List[Any],Set[Plumber]] = {
    combinationLists.foldLeft(Map.empty[List[Any],Set[Plumber]])((map, list) => map + (list -> findIntersection(list,plumbers)))
  }

  private def findIntersection(list: List[Any], plumbers: mutable.Map[Any, ListBuffer[Plumber]]): Set[Plumber] = {
    list.foldLeft(Set.empty[Plumber])( (set,plumber) => {
      val indexedSet : Set[Plumber] = Set(plumbers.get(plumber).get: _*)
      if(set.isEmpty){
        indexedSet
      }else {
        set.intersect(indexedSet)
      }
    })
  }
}
