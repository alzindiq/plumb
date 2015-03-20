package com.alzindiq.cluster

import com.alzindiq.Plumber
import Plumber

import scala.collection.mutable


object PenaltyCalculator {

  def calculatePenalty(bucket : String, plumbers : Set[Plumber], neighbouSimSumMap : Map[Plumber, Double], precalcCohe : Double, similarities : Map[Plumber,Map[Plumber,Double]]) : Double = {
    precalcCohe + correlation(bucket, plumbers, neighbouSimSumMap, Math.abs(1-precalcCohe))
  }

  def calculatePenalty(bucket : String, plumbers : Set[Plumber], neighbouSimSumMap : Map[Plumber, Double], similarities : Map[Plumber,Map[Plumber,Double]]) : Double = {
    var cohe =  cohesion(bucket, plumbers, similarities)
    val corr= correlation(bucket, plumbers, neighbouSimSumMap, Math.abs(1-cohe))
    cohe + corr
  }

 def cohesion(bucket : String, plumbers : Set[Plumber], similarities : Map[Plumber,Map[Plumber,Double]]) = {
    if(plumbers.size <= 1) {
      0d
    }else {
      var cohe = 0d
      val list = plumbers.toList
      val range = 0 to list.size-1

      for(i <- range; j <- range
          if j>i){
        val simM = similarities.getOrElse(list(i),Map.empty) // splitting it boosts performance
        cohe = cohe + 2 * (1 -  simM.getOrElse(list(j), 0.0))
      }
      cohe
    }
  }

  private def correlation(bucket : String, plumbers : Set[Plumber], neighbouSimSumMap : Map[Plumber, Double], simToSelf : Double) : Double = plumbers.map(neighbouSimSumMap.getOrElse(_,0d)).sum - simToSelf
}
