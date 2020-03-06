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

package org.apache.spark.storage.memory

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId


class LFUCache extends Logging with BlockCache {

  val CACHE_SIZE = 10
  private var min = -1
  private val cachedData = new mutable.HashMap[BlockId, MemoryEntry[_]]
  private val keyToCount = new util.HashMap[BlockId, Int]
  private val countToKeys = new util.HashMap[Int, util.LinkedHashSet[BlockId]]

  def get(blockId: BlockId): MemoryEntry[_] = {
    val key = blockId

    if (!cachedData.contains(key)) return null

    val count = keyToCount.get(key)

    countToKeys.get(count).remove(key)

    if (count == min && countToKeys.get(count).size() == 0) min += 1

    putCount(key, count + 1)

    cachedData(key)
  }

  private def putCount(key: BlockId, count: Int): Unit = {
    keyToCount.put(key, count)
    if (!countToKeys.containsKey(count)) {
      countToKeys.put(count, new util.LinkedHashSet[BlockId]())
    }
    countToKeys.get(count).add(key)
  }

  def put(blockId: BlockId, entry: MemoryEntry[_]): Unit = {

    val key = blockId

    if (cachedData.contains(key)) {
      cachedData.put(key, entry)
      get(blockId)
      return
    }

    if (cachedData.size >= CACHE_SIZE) {
      remove(countToKeys.get(min).iterator().next())
    }

    min = 1
    putCount(key, min)
    cachedData.put(key, entry)

  }

  def remove(key: BlockId): MemoryEntry[_] = {
    countToKeys.get(min).remove(key)
    cachedData.remove(key).get
  }


  def contains(blockId: BlockId): Boolean = {
    cachedData.contains(blockId)
  }

  def getIterator: util.Iterator[(BlockId, MemoryEntry[_])] = {
    cachedData.iterator.asJava
  }

  def clear(): Unit = {
    cachedData.clear()
    keyToCount.clear()
    countToKeys.clear()
    min = -1
  }

  def isEmpty: Boolean = {
    cachedData.isEmpty
  }
}
