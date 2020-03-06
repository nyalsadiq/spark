/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.caching

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.CachedData

class FIFOCache(cacheSize: Int) extends Logging with DatasetCache {

  private val cachedData = new java.util.ArrayDeque[CachedData]
  private val index = new util.HashMap[Int, CachedData]()

  val CACHE_SIZE: Int = cacheSize

  def add(item: CachedData): Unit = {
    if (index.containsKey(item.plan.semanticHash())) {
      return
    }

    if (cachedData.size >= CACHE_SIZE) {
      logWarning("FIFO cache full")

      val toRemove = cachedData.poll()
      index.remove(toRemove.plan.semanticHash())
      toRemove.cachedRepresentation.cachedColumnBuffers.unpersist()
    }

    cachedData.addLast(item)
    index.put(item.plan.semanticHash(), item)
  }

  def uncacheQuery(plan: LogicalPlan, blocking: Boolean): Unit = {
    val it = cachedData.iterator()
    while (it.hasNext) {
      val cd = it.next()
      if (cd.plan.find(_.sameResult(plan)).isDefined) {
        index.remove(cd.plan.semanticHash())
        cd.cachedRepresentation.cachedColumnBuffers.unpersist(blocking)
        it.remove()
      }
    }
  }

  def get(item: CachedData): Option[CachedData] = {
    if (index.containsKey(item.plan.semanticHash())) {
      Option.apply(index.get(item.plan.semanticHash()))
    }
    Option.empty
  }

  def get(item: LogicalPlan): Option[CachedData] = {
    if (index.containsKey(item.semanticHash())) {
      Option.apply(index.get(item.semanticHash()))
    }
    Option.empty
  }

  def clear(): Unit = {
    index.clear()
    cachedData.clear()
  }

  def isEmpty: Boolean = {
    cachedData.isEmpty
  }

  def getIterator: util.Iterator[CachedData] = cachedData.iterator()
}
