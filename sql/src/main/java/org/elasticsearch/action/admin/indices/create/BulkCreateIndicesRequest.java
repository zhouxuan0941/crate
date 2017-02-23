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

package org.elasticsearch.action.admin.indices.create;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class BulkCreateIndicesRequest extends AcknowledgedRequest<BulkCreateIndicesRequest> {

    private Collection<String> indices = ImmutableList.of();
    private UUID jobId;

    /**
     * Constructs a new request to create indices with the specified names.
     */
    public BulkCreateIndicesRequest(Collection<String> indices, UUID jobId) {
        this.indices = indices;
        this.jobId = jobId;
    }

    public BulkCreateIndicesRequest() {
    }

    public Collection<String> indices() {
        return indices;
    }

    public UUID jobId() {
        return jobId;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        jobId = new UUID(in.readLong(), in.readLong());
        int numIndices = in.readVInt();
        List<String> indicesList = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            indicesList.add(in.readString());
        }
        this.indices = indicesList;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(indices.size());
        for (String index : indices) {
            out.writeString(index);
        }
    }

}
