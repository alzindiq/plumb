package com.alzindiq.bucketer

import com.alzindiq.Plumber
import Plumber

trait Bucketer {
  def bucket(toBeBucketed : List[Plumber]) : Map[Any,Set[Plumber]]
}

class SimpleWordBucketer(bucketingFunction : (List[Plumber]) => Map[Any,Set[Plumber]]) extends Bucketer {
  override def bucket(toBeBucketed : List[Plumber]): Map[Any, Set[Plumber]] = {
    bucketingFunction.apply(toBeBucketed)
  }
}
