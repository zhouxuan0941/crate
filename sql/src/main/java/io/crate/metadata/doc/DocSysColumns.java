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

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Map;
import java.util.function.BiConsumer;

public class DocSysColumns {


    public static final ColumnIdent ID = new ColumnIdent("_id");
    public static final ColumnIdent VERSION = new ColumnIdent("_version");
    public static final ColumnIdent SCORE = new ColumnIdent("_score");
    public static final ColumnIdent UID = new ColumnIdent("_uid");
    public static final ColumnIdent DOC = new ColumnIdent("_doc");
    public static final ColumnIdent RAW = new ColumnIdent("_raw");
    public static final ColumnIdent DOCID = new ColumnIdent("_docid");

    public static final ImmutableMap<ColumnIdent, DataType> COLUMN_IDENTS = ImmutableMap.<ColumnIdent, DataType>builder()
        .put(ID, DataTypes.STRING)
        .put(VERSION, DataTypes.LONG)
        .put(SCORE, DataTypes.FLOAT)
        .put(UID, DataTypes.STRING)
        .put(DOC, DataTypes.OBJECT)
        .put(RAW, DataTypes.STRING)
        .put(DOCID, DataTypes.LONG)
        .build();

    private static final ImmutableMap<ColumnIdent, String> LUCENE_COLUMN_NAMES = ImmutableMap.<ColumnIdent, String>builder()
        .put(RAW, "_source")
        .put(ID, UID.name())
        .build();

    private static Reference newInfo(TableIdent table, ColumnIdent column, DataType dataType) {
        return new Reference(new ReferenceIdent(table, column), RowGranularity.DOC, dataType);
    }

    /**
     * Calls {@code consumer} for each sys column with a reference containing {@code tableIdent}
     */
    public static void forTable(TableIdent tableIdent, BiConsumer<ColumnIdent, Reference> consumer) {
        for (Map.Entry<ColumnIdent, DataType> entry : COLUMN_IDENTS.entrySet()) {
            ColumnIdent columnIdent = entry.getKey();
            consumer.accept(columnIdent, newInfo(tableIdent, columnIdent, entry.getValue()));
        }
    }

    public static Reference forTable(TableIdent table, ColumnIdent column) {
        return newInfo(table, column, COLUMN_IDENTS.get(column));
    }

    public static String nameForLucene(ColumnIdent ident) {
        String name = LUCENE_COLUMN_NAMES.get(ident);
        if (name == null) {
            name = ident.name();
        }
        return name;
    }
}
