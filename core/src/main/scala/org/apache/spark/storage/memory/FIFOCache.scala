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

import org.apache.spark.storage.BlockId

class FIFOCache extends BlockCache {

  private val cachedData = new java.util.ArrayDeque[(BlockId, MemoryEntry[_])]
  private val index = new util.HashMap[BlockId, MemoryEntry[_]]()

  def get(blockId: BlockId): MemoryEntry[_] = {
     index.get(blockId)
  }

  def put(blockId: BlockId, entry: MemoryEntry[_]): Unit = {
    cachedData.addLast((blockId, entry))
    index.put(blockId, entry)
  }

  def contains(blockId: BlockId): Boolean = {
    index.containsKey(blockId)
  }

  def remove(blockId: BlockId): MemoryEntry[_] = {
    cachedData.removeIf(_._1.equals(blockId))
    index.remove(blockId)
  }

  def getIterator: util.Iterator[(BlockId, MemoryEntry[_])] = {
    cachedData.iterator()
  }

  def clear(): Unit = {
    cachedData.clear()
    index.clear()
  }

  def isEmpty: Boolean = {
    cachedData.isEmpty
  }
}
