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
import org.apache.spark.sql.execution.CachedData


class LFUCache extends Logging {

  private val CACHE_SIZE = 10
  private var min = -1
  private val cachedData = new util.HashMap[Int, CachedData]
  private val keyToCount = new util.HashMap[Int, Int]
  private val countToKeys = new util.HashMap[Int, util.LinkedHashSet[Integer]]

  def get(item: CachedData): CachedData = {
    val key = item.plan.semanticHash()

    if (!cachedData.containsKey(key)) return null

    val count = keyToCount.get(key)
    countToKeys.get(count).remove(key)

    if (count == min && countToKeys.get(count).size() == 0) min += 1

    putCount(key, count + 1)

    cachedData.get(key)
  }

  def add(item: CachedData): Unit = {
    val key = item.plan.semanticHash()

    if (cachedData.containsKey(key)) {
      cachedData.put(key, item)
      get(item)
      return
    }

    if (cachedData.size() >= CACHE_SIZE) {
      evict(countToKeys.get(min).iterator().next())
    }

    min = 1
    putCount(key, min)
    cachedData.put(key, item)
  }

  private def evict(key: Int) {
    countToKeys.get(min).remove(key)
    cachedData.remove(key).cachedRepresentation.cachedColumnBuffers.unpersist()
  }

  private def putCount(key: Int, count: Int): Unit = {
    keyToCount.put(key, count)
    countToKeys.computeIfAbsent(count, _ => new util.LinkedHashSet[Integer]())
    countToKeys.get(count).add(key)
  }

  def getCachedData: util.Collection[CachedData] = cachedData.values()

}
