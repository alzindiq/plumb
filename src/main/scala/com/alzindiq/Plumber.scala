package com.alzindiq

import com.alzindiq.similarity.AbstractSimilarityRanker

case class Plumber(attributes: Map[String, Any]){
  def getId : Option[Any] = attributes.get(AbstractSimilarityRanker.plumbId)
}

