package com.alzindiq.similarity

import com.alzindiq.Plumber
import Plumber

class DeltaBruteSimilarityRanker (bFilters : Seq[Plumber => Boolean], cFilters : Seq[Plumber => Boolean],
                          stitchCheckers : Seq[(Plumber, Plumber) => Double], dedup : Boolean, acceptanceThreshold : Double )
  extends BruteForceSimilarityRanker(bFilters,cFilters,stitchCheckers, dedup, acceptanceThreshold) with IncrementalSimilarityRanker {

  def this( stitchCheckers : Seq[(Plumber, Plumber) => Double], dedup: Boolean, acceptanceThreshold : Double = 1.0) = this(Seq.empty[Plumber => Boolean],Seq.empty[Plumber => Boolean], stitchCheckers, dedup, acceptanceThreshold)

  override def increment(newItems: Seq[Plumber], updatedItems: Seq[Plumber], deletedItems: Seq[Plumber]): Unit = {
    if(lastRunSimilarities.isEmpty){ // no previous stitches exist
      Map.empty
    }else{
      val filteredDeleted = deletedItems.filter(b => applyAllFilters(bFilters, b))
      allItems=allItems.filter(p => !filteredDeleted.contains(p))
      treatDeletedItems(filteredDeleted)

      if(dedup){
        val filteredUpdated = updatedItems.filter(c => applyAllFilters(cFilters, c))
        val filteredNew = newItems.filter(c => applyAllFilters(cFilters, c))
        treatUpdatedItems(filteredUpdated)
        calculateSimilarity(mergePlumbers(filteredNew, filteredUpdated), allItems)
      }else {
        val filteredUpdated = updatedItems.filter(b => applyAllFilters(bFilters, b))
        val filteredNew = newItems.filter(b => applyAllFilters(bFilters, b))
        calculateSimilarity(mergePlumbers(filteredNew, filteredUpdated), allItems)
      }
    }
  }

  private def treatDeletedItems(filteredDeleted: Seq[Plumber]): Unit = {
    lastRunSimilarities=lastRunSimilarities.filter(p => !filteredDeleted.contains(p._1))
    for(b <- lastRunSimilarities.keySet){
      for(c <- lastRunSimilarities.get(b).get.keySet) {
        if(filteredDeleted.contains(c)) {
          lastRunSimilarities(b)=lastRunSimilarities.get(b).get.-(c)
          if(!lastRunSimilarities.get(c).isEmpty){
            lastRunSimilarities(c)=lastRunSimilarities.get(c).get.-(b)
          }
        }
      }
    }
  }

  private def treatUpdatedItems(filteredUpdated: Seq[Plumber]): Unit = {
    val sortedFilteredUpdated = filteredUpdated.sortBy(_.getId.get.toString).toList
    val filteredUpdatedKeys = sortedFilteredUpdated.map(p => p.getId.get).toList
    allItems.foreach(p => {
      val indx = filteredUpdatedKeys.indexOf(p.getId.get)
      if (indx != -1) {
        // replace with updated
        sortedFilteredUpdated.patch(indx, Seq(sortedFilteredUpdated(indx)), 1)
      }
    })
    lastRunSimilarities=lastRunSimilarities.filter(m => !filteredUpdatedKeys.contains(m._1.getId.get))
  }

  private def mergePlumbers(newItems: Seq[Plumber], updatedItems: Seq[Plumber]) : Seq[Plumber] = {
    newItems.toSet.union(updatedItems.toSet).toSeq
  }
}
