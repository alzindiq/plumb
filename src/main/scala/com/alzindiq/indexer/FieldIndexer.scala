package com.alzindiq.indexer

import com.alzindiq.Plumber
import Plumber
import scala.collection.mutable

class FieldIndexer(stopValues : Option[List[Any]], trimming : Any => Any = identity ) {

  var indices : mutable.Map[String,mutable.Map[Any,mutable.ListBuffer[Plumber]]] = mutable.Map.empty

  def createIndices[A <: Plumber](creator : Map[String, A => List[Any]], data : Set[A]) : Unit = {
    val indexNames =  creator.toList.map(_._1).toList
    val indexFieldNamesAndExtractor = creator.toList.map(_._2).toList
    if (indexNames.size == indexFieldNamesAndExtractor.size) {
      indexNames.foreach(name => indices.update(name, mutable.Map.empty))
      for (p <- data) {
        for (i <- 0 to indexNames.size-1) {
          val toBeIndexed = indexFieldNamesAndExtractor(i).apply(p)
          if(!toBeIndexed.headOption.isEmpty) {
            if(toBeIndexed.head.isInstanceOf[List[Any]]){
              toBeIndexed.foreach(list => trimStopUpdate(indexNames, p, i, list.asInstanceOf[List[Any]]))
            }else {
              toBeIndexed.foreach(trimStopUpdate(indexNames, p, i, _))
            }
          }
        }
      }
    }
  }

  def trimStopUpdate[A <: Plumber](indexNames: List[String], p: A, i: Int, indexedValue: Any): Unit = {
    if(indexedValue.isInstanceOf[List[Any]]){
      val trimmedIndexedValue= indexedValue.asInstanceOf[List[Any]].map(trimming.apply(_)).toList
      stop(indexNames, p, i, trimmedIndexedValue)
    }else {
      val trimmedIndexedValue = trimming.apply(indexedValue)
      stop(indexNames, p, i, trimmedIndexedValue)
    }
  }

  def stop[A <: Plumber](indexNames: List[String], p: A, i: Int, trimmedIndexedValue : Any): Unit = {
    if (!stopValues.isEmpty) {
      if(trimmedIndexedValue.isInstanceOf[List[Any]]){
        if(trimmedIndexedValue.asInstanceOf[List[Any]].foldLeft(true)((acc,any) => acc && !stopValues.get.contains(any))){
          updateIndex(indexNames, p, i, trimmedIndexedValue)
        }
      }else {
        if (!stopValues.get.contains(trimmedIndexedValue)) {
          updateIndex(indexNames, p, i, trimmedIndexedValue)
        }
      }
    } else {
      updateIndex(indexNames, p, i, trimmedIndexedValue)
    }
  }

  private def updateIndex(indexNames: List[String], p: Plumber, i: Int, trimmedIndexedValue: Any): Unit = {
    val invertedIndx = indices.get(indexNames(i)).get.get(trimmedIndexedValue).getOrElse(mutable.ListBuffer.empty)
    invertedIndx+=(p)
    indices.get(indexNames(i)).get += trimmedIndexedValue -> invertedIndx
  }
}
