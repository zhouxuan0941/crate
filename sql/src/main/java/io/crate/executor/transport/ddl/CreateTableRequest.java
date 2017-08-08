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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;

public class CreateTableRequest extends AcknowledgedRequest<CreateTableRequest> {

    private TableIdent tableIdent;
    private boolean isPartitioned;
    private Settings settings = EMPTY_SETTINGS;
    private final Map<String, String> mappings = new HashMap<>();

    CreateTableRequest() {
    }

    public CreateTableRequest(TableIdent tableIdent, boolean isPartitioned, Settings settings) {
        this.tableIdent = tableIdent;
        this.isPartitioned = isPartitioned;
        this.settings = settings;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public boolean isPartitioned() {
        return isPartitioned;
    }

    public Settings settings() {
        return settings;
    }

    public Map<String, String> mappings() {
        return mappings;
    }

    /**
     * Adds mapping that will be added when the table gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     */
    @SuppressWarnings("unchecked")
    public CreateTableRequest mapping(String type, Map source) {
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        // wrap it in a type map if its not
        if (source.size() != 1 || !source.containsKey(type)) {
            source = MapBuilder.<String, Object>newMapBuilder().put(type, source).map();
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            return mapping(type, builder.bytes(), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Adds mapping that will be added when the table gets created.
     *
     * @param type   The mapping type
     * @param source The mapping source
     * @param xContentType the content type of the mapping source
     */
    private CreateTableRequest mapping(String type, BytesReference source, XContentType xContentType) {
        if (mappings.containsKey(type)) {
            throw new IllegalStateException("mappings for type \"" + type + "\" were already defined");
        }
        Objects.requireNonNull(xContentType);
        try {
            mappings.put(type, XContentHelper.convertToJson(source, false, false, xContentType));
            return this;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to convert to json", e);
        }
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
        isPartitioned = in.readBoolean();
        settings = readSettingsFromStream(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            final String type = in.readString();
            String source = in.readString();
            if (in.getVersion().before(Version.V_5_3_0)) {
                // we do not know the content type that comes from earlier versions so we autodetect and convert
                source = XContentHelper.convertToJson(new BytesArray(source), false, false, XContentFactory.xContentType(source));
            }
            mappings.put(type, source);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeTimeout(out);
        tableIdent.writeTo(out);
        out.writeBoolean(isPartitioned);
        writeSettingsToStream(settings, out);
        out.writeVInt(mappings.size());
        for (Map.Entry<String, String> entry : mappings.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
    }
}
