/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.doc.lucene;

import io.crate.exceptions.UnhandledServerException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.types.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.MapperService;

import java.util.Locale;

public class LuceneReferenceResolver implements ReferenceResolver<LuceneCollectorExpression<?>> {

    private final @Nullable MapperService mapperService;

    private final static NullValueCollectorExpression NULL_COLLECTOR_EXPRESSION = new NullValueCollectorExpression();

    public LuceneReferenceResolver(@Nullable MapperService mapperService) {
        this.mapperService = mapperService;
    }

    @Override
    public LuceneCollectorExpression<?> getImplementation(Reference refInfo) {
        assert refInfo.granularity() == RowGranularity.DOC;

        String name = refInfo.column().name();
        if (RawCollectorExpression.COLUMN_NAME.equals(name)){
            if (refInfo.column().isColumn()){
                return new RawCollectorExpression();
            } else {
                // TODO: implement an Object source expression which may support subscripts
                throw new UnsupportedFeatureException(
                        String.format(Locale.ENGLISH, "_source expression does not support subscripts %s",
                        refInfo.column().fqn()));
            }
        } else if (IdCollectorExpression.COLUMN_NAME.equals(name)) {
            return new IdCollectorExpression();
        } else if (DocCollectorExpression.COLUMN_NAME.equals(name)) {
            return DocCollectorExpression.create(refInfo);
        } else if (DocIdCollectorExpression.COLUMN_NAME.equals(name)) {
            return new DocIdCollectorExpression();
        } else if (ScoreCollectorExpression.COLUMN_NAME.equals(name)) {
            return new ScoreCollectorExpression();
        }

        String fqn = refInfo.column().fqn();
        if (this.mapperService != null && mapperService.smartNameFieldType(fqn) == null) {
            return NULL_COLLECTOR_EXPRESSION;
        }

        switch (refInfo.valueType().id()) {
            case ByteType.ID:
                return new ByteColumnReference(fqn);
            case ShortType.ID:
                return new ShortColumnReference(fqn);
            case IpType.ID:
                return new IpColumnReference(fqn);
            case StringType.ID:
                return new BytesRefColumnReference(fqn);
            case DoubleType.ID:
                return new DoubleColumnReference(fqn);
            case BooleanType.ID:
                return new BooleanColumnReference(fqn);
            case ObjectType.ID:
                return new ObjectColumnReference(fqn);
            case FloatType.ID:
                return new FloatColumnReference(fqn);
            case LongType.ID:
            case TimestampType.ID:
                return new LongColumnReference(fqn);
            case IntegerType.ID:
                return new IntegerColumnReference(fqn);
            case GeoPointType.ID:
                return new GeoPointColumnReference(fqn);
            case GeoShapeType.ID:
                return new GeoShapeColumnReference(fqn);
            default:
                throw new UnhandledServerException(String.format(Locale.ENGLISH, "unsupported type '%s'", refInfo.valueType().getName()));
        }
    }

    private static class NullValueCollectorExpression extends LuceneCollectorExpression<Void> {

        @Override
        public Void value() {
            return null;
        }
    }
}
