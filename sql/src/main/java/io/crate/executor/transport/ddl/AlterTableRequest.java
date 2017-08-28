/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport.ddl;

import io.crate.metadata.TableIdent;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

public class AlterTableRequest extends AcknowledgedRequest<AlterTableRequest> {

    private TableIdent tableIdent;
    @Nullable
    private String partitionIndexName;
    private boolean isPartitioned = false;
    private boolean excludePartitions = false;
    private Settings settings = EMPTY_SETTINGS;
    private String source;

    AlterTableRequest() {
    }

    public AlterTableRequest(TableIdent tableIdent,
                             @Nullable String partitionIndexName,
                             boolean isPartitioned,
                             boolean excludePartitions) {
        this.tableIdent = tableIdent;
        this.partitionIndexName = partitionIndexName;
        this.isPartitioned = isPartitioned;
        this.excludePartitions = excludePartitions;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    @Nullable
    public String partitionIndexName() {
        return partitionIndexName;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public boolean excludePartitions() {
        return excludePartitions;
    }

    public AlterTableRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public Settings settings() {
        return settings;
    }

    public AlterTableRequest source(Map mappingSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            //noinspection unchecked
            builder.map(mappingSource);
            this.source = XContentHelper.convertToJson(new BytesArray(builder.string()), false, false, XContentType.JSON);
            return this;
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + mappingSource + "]", e);
        }
    }

    @Nullable
    public String source() {
        return source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (tableIdent == null) {
            validationException = addValidationError("table ident must not be null", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        readTimeout(in);
        tableIdent = new TableIdent(in);
        partitionIndexName = in.readOptionalString();
        isPartitioned = in.readBoolean();
        excludePartitions = in.readBoolean();
        settings = readSettingsFromStream(in);
        source = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeTimeout(out);
        tableIdent.writeTo(out);
        out.writeOptionalString(partitionIndexName);
        out.writeBoolean(isPartitioned);
        out.writeBoolean(excludePartitions);
        writeSettingsToStream(settings, out);
        out.writeOptionalString(source);
    }
}
