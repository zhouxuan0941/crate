/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.join;

import org.apache.lucene.util.LongBitSet;

/**
 * This BitSet is used to mark matched rows between left and right in {@link NestedLoopOperation}
 * <p>
 * Each bit true if the rows in the respective position are matched and therefore we need
 * a structure capable of holding <pre>long</pre> size of bits so java.util.BitSet cannot be used.
 * <p>
 * We chose to use {@link LongBitSet} from Lucence and add another layer on top in
 * order to further optimize performance by growing the capacity of the backing array
 * by double each time size is reached.
 */
class LuceneLongBitSetWrapper {
    private long size = 1024;
    private LongBitSet bitSet = new LongBitSet(size);

    void set(long idx) {
        if (idx >= size) {
            size *= 2;
            bitSet = LongBitSet.ensureCapacity(bitSet, size);
        }
        bitSet.set(idx);
    }

    boolean get(long idx) {
        return bitSet.get(idx);
    }
}
