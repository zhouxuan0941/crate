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

package io.crate.operation.scalar;

import com.google.common.collect.Sets;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * CollectScalar should run within the COLLECT phase and can gather any amount of inputs.
 * It always returns TRUE if the collection was successful, otherwise FALSE.
 */
public class CollectScalar extends Scalar<Boolean, Object> {

    private static final FuncParams PARAMS = FuncParams.builder().withIndependentVarArgs(Param.ANY).build();
    private static final Set<FunctionInfo.Feature> FEATURES = Sets.immutableEnumSet(FunctionInfo.Feature.DETERMINISTIC, FunctionInfo.Feature.COLLECT_PHASE);
    private static final Logger LOGGER = Loggers.getLogger(CollectScalar.class);
    public static final String NAME = "_collect";
    private final FunctionInfo info;

    private CollectScalar(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public Boolean evaluate(Input<Object> ...args) {
        /*
         * Do something with the data here ...
         *
         * Example:
         * <code>
         *     SELECT _collect(_raw) FROM t1;
         * </code>
         * would result in calling this function with a single argument (args[0]) that contains the raw JSON data of the
         * document as BytesRef object.
         */
        String repr = Arrays.stream(args)
            .map(x -> String.format("%s[%s]", x.getClass().getSimpleName(), BytesRefs.toString(x.value())))
            .reduce("Row:", (a, b) -> a + " " + b);
        LOGGER.info(repr);
        return true;
    }

    public static FunctionInfo createInfo(List<DataType> dataTypes) {
        return new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.BOOLEAN, FunctionInfo.Type.SCALAR, FEATURES);
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, RESOLVER);
    }

    private static final FunctionResolver RESOLVER = new BaseFunctionResolver(PARAMS) {

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return new CollectScalar(createInfo(dataTypes));
        }
    };
}
