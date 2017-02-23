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

package io.crate.blob;

import io.crate.common.Hex;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class PutChunkRequest extends BlobTransferRequest<PutChunkRequest> implements IPutChunkRequest {

    private byte[] digest;
    private long currentPos;

    public PutChunkRequest() {
    }

    public PutChunkRequest(String index, byte[] digest, UUID transferId,
                           BytesArray content, long currentPos, boolean last) {
        super(index, transferId, content, last);
        this.digest = digest;
        this.currentPos = currentPos;
    }

    public String digest() {
        return Hex.encodeHexString(digest);
    }

    public long currentPos() {
        return currentPos;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        digest = new byte[20];
        in.read(digest);
        currentPos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.write(digest);
        out.writeVLong(currentPos);
    }
}
