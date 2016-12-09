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

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.RowType;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.MoreObjects.firstNonNull;

public class MappingParser {

    private static final Set<String> COMPOSITE_TYPES = Sets.newHashSet("array", "object");

    static class Context {

        private final MetaInfo metaInfo;
        private final RowType rowType;
        private final ColumnPolicy columnPolicy;

        public Context(MetaInfo metaInfo, RowType rowType, ColumnPolicy columnPolicy) {
            this.metaInfo = firstNonNull(metaInfo, MetaInfo.EMPTY);
            this.rowType = firstNonNull(rowType, RowType.EMPTY);
            this.columnPolicy = columnPolicy;
        }

        public ColumnPolicy columnPolicy() {
            return columnPolicy;
        }

        public List<ColumnIdent> primaryKeys() {
            return metaInfo.primaryKeys;
        }

        public ColumnIdent clusteredBy() {
            ColumnIdent routing = metaInfo.routing;
            if (routing == null) {
                if (metaInfo.primaryKeys.size() == 1) {
                    return metaInfo.primaryKeys.get(0);
                }
                return DocSysColumns.ID;
            }
            return routing;
        }

        public List<ColumnIdent> partitionedBy() {
            return metaInfo.partitionedBy;
        }

        public List<Reference> columns() {
            return null;
        }

        public List<Reference> partitionedByColumns() {
            return Collections.emptyList();
        }

        public List<GeneratedReference> generatedColumns() {
            return Collections.emptyList();
        }

        public ImmutableMap<ColumnIdent, IndexReference> indexColumns() {
            return ImmutableMap.of();
        }

        public ImmutableMap<ColumnIdent, Reference> createReferences(TableIdent tableIdent) {
            ImmutableMap.Builder<ColumnIdent, Reference> builder = ImmutableMap.builder();
            addSysColumns(tableIdent, builder);
            createReferences(tableIdent, rowType, builder, null);
            return builder.build();
        }

        private static void addSysColumns(TableIdent tableIdent, ImmutableMap.Builder<ColumnIdent, Reference> builder) {
            for (Map.Entry<ColumnIdent, DataType> entry : DocSysColumns.COLUMN_IDENTS.entrySet()) {
                ColumnIdent key = entry.getKey();
                builder.put(key, DocSysColumns.forTable(tableIdent, key));
            }
        }

        private static void createReferences(TableIdent tableIdent, RowType rowType, ImmutableMap.Builder<ColumnIdent, Reference> builder, @Nullable ColumnIdent parent) {
            Iterator<String> nameIt = rowType.names().iterator();
            Iterator<DataType> typeIt = rowType.types().iterator();
            while (nameIt.hasNext()) {
                ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
                ColumnIdent columnIdent;
                if (parent == null) {
                    columnIdent = new ColumnIdent(nameIt.next());
                } else {
                    columnIdent = ColumnIdent.getChild(parent, nameIt.next());
                }
                DataType dataType = typeIt.next();
                if (dataType instanceof RowType) {
                    RowType rt = (RowType) dataType;
                    createReferences(tableIdent, rt, builder, columnIdent);
                    dataType = DataTypes.OBJECT;
                    columnPolicy = rt.columnPolicy();
                }
                Reference reference = new Reference(
                    new ReferenceIdent(tableIdent, columnIdent),
                    RowGranularity.DOC,
                    dataType,
                    columnPolicy,
                    Reference.IndexType.NOT_ANALYZED,
                    false
                );
                builder.put(columnIdent, reference);
            }
        }
    }

    private static class MetaInfo {

        static final MetaInfo EMPTY = new MetaInfo(null, null, null, null);

        private final List<ColumnIdent> primaryKeys;
        private final ColumnIdent routing;
        private final List<ColumnIdent> partitionedBy;
        private final Map<ColumnIdent, String> generatedColumns;

        MetaInfo(@Nullable List<ColumnIdent> primaryKeys,
                 ColumnIdent routing,
                 @Nullable List<ColumnIdent> partitionedBy,
                 @Nullable Map<ColumnIdent, String> generatedColumns) {

            this.primaryKeys = firstNonNull(primaryKeys, Collections.emptyList());
            this.routing = routing;
            this.partitionedBy = firstNonNull(partitionedBy, Collections.emptyList());
            this.generatedColumns = firstNonNull(generatedColumns, Collections.emptyMap());
        }
    }

    public static Context parse(MappingMetaData mapping) throws IOException {
        XContentParser parser = XContentHelper.createParser(mapping.source().compressedReference());

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case "default":
                    token = parser.nextToken();
                    return parseDefaultMapping(parser, token);
                default:
                    parser.skipChildren();
            }
        }
        throw new IllegalArgumentException("Invalid mapping received");
    }

    private static Context parseDefaultMapping(XContentParser parser, XContentParser.Token token) throws IOException {
        MetaInfo metaInfo = null;
        ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
        RowType rowType = null;
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case "dynamic":
                    columnPolicy = ColumnPolicy.of(parser.textOrNull());
                    break;
                case "_meta":
                    token = parser.nextToken(); // skip START_OBJECT
                    metaInfo = parseMeta(parser, token);
                    break;
                case "properties":
                    token = parser.nextToken(); // skip START_OBJECT
                    rowType = parseProperties(parser, token);
                    break;
                default:
                    parser.skipChildren();
            }
        }
        return new Context(metaInfo, rowType, columnPolicy);
    }

    private static MetaInfo parseMeta(XContentParser parser, XContentParser.Token token) throws IOException {
        ColumnIdent routing = null;
        List<ColumnIdent> primaryKeys = null;
        List<ColumnIdent> partitionedBy = null;
        Map<ColumnIdent, String> generatedColumns = null;

        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = parser.currentName();
            token = parser.nextToken();

            switch (fieldName) {
                case "routing":
                    routing = ColumnIdent.fromPath(parser.text());
                    break;
                case "primary_keys":
                    primaryKeys = readColumnIdentList(parser, token);
                    break;
                case "partitioned_by":
                    // TODO:
                    parser.skipChildren();
                    partitionedBy = Collections.emptyList();
                    break;
                case "generated_columns":
                    // TODO:
                    parser.skipChildren();
                    break;
                case "constraints":
                    // TODO:
                    parser.skipChildren();
                    break;

                default:
                    // TODO: log warning?
                    parser.skipChildren();
            }
        }

        return new MetaInfo(primaryKeys, routing, partitionedBy, generatedColumns);
    }

    private static List<ColumnIdent> readColumnIdentList(XContentParser parser, XContentParser.Token token) throws IOException {
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        }
        List<ColumnIdent> columns = new ArrayList<>();
        for(; token != XContentParser.Token.END_ARRAY; token = parser.nextToken()) {
            assert token.isValue() : "token must be a value token";
            columns.add(ColumnIdent.fromPath(parser.text()));
        }
        return columns;
    }

    private static RowType parseProperties(XContentParser parser, XContentParser.Token token) throws IOException {
        List<String> names = new ArrayList<>();
        List<DataType> types = new ArrayList<>();
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = parser.currentName();
            names.add(fieldName);
            parser.nextToken();

            token = parser.nextToken();
            types.add(parseType(parser, token));
        }
        return new RowType(names, types, ColumnPolicy.DYNAMIC);
    }

    private static DataType parseType(XContentParser parser, XContentParser.Token token) throws IOException {
        DataType dataType = DataTypes.UNDEFINED;
        ColumnPolicy columnPolicy = null;
        for (; token == XContentParser.Token.FIELD_NAME; token = parser.nextToken()) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case "type":
                    String type = parser.text();
                    if (!COMPOSITE_TYPES.contains(type)) {
                        dataType = DataTypes.ofName(type);
                    }
                    break;
                case "inner":
                    token = parser.nextToken();
                    dataType = new ArrayType(parseType(parser, token));
                    break;
                case "dynamic":
                    columnPolicy = ColumnPolicy.of(parser.text());
                    break;
                case "properties":
                    token = parser.nextToken();
                    dataType = parseProperties(parser, token);
                    break;
                default:
                    parser.skipChildren();
            }
        }
        if (columnPolicy != null) {
            assert dataType instanceof RowType : "dataType must be rowType if columnPolicy got set, was: " + dataType;
            ((RowType) dataType).columnPolicy(columnPolicy);
        }
        return dataType;
    }
}
