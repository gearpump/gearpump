package org.apache.gearpump.scheduler

object ResourceCalculator {
  def clone(res: Resource) = Resource(res.slots)

  def subtract(res1: Resource, res2: Resource) = Resource(res1.slots - res2.slots)

  def add(res1: Resource, res2: Resource) = Resource(res1.slots + res2.slots)

  def compare(res1: Resource, res2: Resource) = res1.slots - res2.slots

  def lessThan(res1: Resource, res2: Resource) = compare(res1, res2) < 0

  def equals(res1: Resource, res2: Resource) = compare(res1, res2) == 0

  def greaterThan(res1: Resource, res2: Resource) = compare(res1, res2) > 0

  def min(res1: Resource, res2: Resource) = if (res1.slots < res2.slots) res1 else res2
}