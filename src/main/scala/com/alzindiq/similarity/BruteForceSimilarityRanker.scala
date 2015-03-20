package com.alzindiq.similarity

import com.alzindiq.Plumber
import Plumber

class BruteForceSimilarityRanker (bFilters : Seq[Plumber => Boolean], cFilters : Seq[Plumber => Boolean],
                          stitchCheckers : Seq[(Plumber, Plumber) => Double], dedup : Boolean, acceptanceThreshold : Double)
  extends AbstractSimilarityRanker(bFilters,cFilters,stitchCheckers, dedup, acceptanceThreshold) {

  def this( stitchCheckers : Seq[(Plumber, Plumber) => Double], dedup: Boolean,  acceptanceThreshold : Double = 1.0) = this(Seq.empty[Plumber => Boolean],Seq.empty[Plumber => Boolean], stitchCheckers, dedup, acceptanceThreshold)

  override def execStitching(filteredB: Seq[Plumber], filteredC: Seq[Plumber]) = stitchCheckers.foreach(c => updateStitchMap(filteredB,filteredC, c))

}
