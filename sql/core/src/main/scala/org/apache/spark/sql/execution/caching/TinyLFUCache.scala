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

import scala.collection.JavaConverters._

import com.github.blemale.scaffeine.{Cache, Scaffeine}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.CachedData


class TinyLFUCache extends Logging with DatasetCache {

  val CACHE_SIZE = 10

  private val cachedData: Cache[Int, CachedData] = Scaffeine()
    .recordStats()
    .maximumSize(CACHE_SIZE)
    .build[Int, CachedData]()

  def get(item: CachedData): Option[CachedData] = {
    cachedData.getIfPresent(item.plan.semanticHash())
  }

  def get(item: LogicalPlan): Option[CachedData] = {
    cachedData.getIfPresent(item.semanticHash())
  }

  def add(item: CachedData): Unit = {
    cachedData.put(item.plan.semanticHash(), item)
  }

  def clear(): Unit = {
    cachedData.asMap().foreach(pair => pair._2.cachedRepresentation.cachedColumnBuffers.unpersist())
    cachedData.invalidateAll()
  }

  def isEmpty: Boolean = cachedData.asMap().isEmpty

  def getIterator: util.Iterator[CachedData] = cachedData.asMap().valuesIterator.asJava

  def uncacheQuery(plan: LogicalPlan, blocking: Boolean): Unit = {
    val it = getIterator
    while (it.hasNext) {
      val cd = it.next()
      if (cd.plan.find(_.sameResult(plan)).isDefined) {
        cachedData.invalidate(cd.plan.semanticHash())
        cd.cachedRepresentation.cachedColumnBuffers.unpersist(blocking)
        it.remove()
      }
    }
  }

}
