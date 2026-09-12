/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.functions.VariantPathParser;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.variant.Variant;

import javax.annotation.Nullable;
import java.util.Optional;

/** Implementation of {@link BuiltInFunctionDefinitions#VARIANT_GET}. */
public class VariantGetFunction extends BuiltInScalarFunction {

    public VariantGetFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.VARIANT_GET, context);
    }

    public @Nullable Object eval(@Nullable Variant variant, String path, DataType targetType) {
        if (variant == null) {
            return null;
        }

        try {
            Optional<VariantPathParser.VariantPathSegment[]> optionalVariantPathSegments = VariantPathParser.parse(path);
            //            path
            return null;
        } catch (Throwable e) {
            throw new TableRuntimeException(
                    String.format("Failed to parse this path: %s", path), e); // liujinkun02 todo
        }
    }
}
