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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses VARIANT paths into object field and array index accesses, following Spark's grammar. */
@Internal
public final class VariantPathParser {

    private static final String DOT_FIELD = "\\.([^.\\[]+)";
    private static final String SINGLE_QUOTED_FIELD = "\\['([^']*)']";
    private static final String DOUBLE_QUOTED_FIELD = "\\[\"([^\"]*)\"]";
    private static final String ARRAY_INDEX = "\\[([0-9]+)]";

    private static final Pattern SEGMENT_PATTERN =
            Pattern.compile(
                    DOT_FIELD
                            + "|"
                            + SINGLE_QUOTED_FIELD
                            + "|"
                            + DOUBLE_QUOTED_FIELD
                            + "|"
                            + ARRAY_INDEX);

    private VariantPathParser() {
    }

    /**
     * Parses a path starting with {@code $}, followed by {@code .name}, {@code ['name']}, {@code
     * ["name"]}, or non-negative array indices such as {@code [0]}.
     *
     * <p>The root path {@code $} produces an empty array. Invalid paths, including indices
     * exceeding {@link Integer#MAX_VALUE}, produce an empty optional. Whitespace is not skipped,
     * and quoted field names are read literally without interpreting escapes.
     */
    public static Optional<VariantPathSegment[]> parse(String path) {
        if (path.isEmpty() || path.charAt(0) != '$') {
            return Optional.empty();
        }

        final List<VariantPathSegment> segments = new ArrayList<>();
        final Matcher matcher = SEGMENT_PATTERN.matcher(path);
        int position = 1;
        while (position < path.length()) {
            matcher.region(position, path.length());
            if (!matcher.lookingAt()) {
                return Optional.empty();
            }

            if (matcher.group(4) != null) {
                try {
                    segments.add(new ArrayExtraction(Integer.parseInt(matcher.group(4))));
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            } else {
                final String key =
                        matcher.group(1) != null
                                ? matcher.group(1)
                                : matcher.group(2) != null ? matcher.group(2) : matcher.group(3);
                segments.add(new ObjectExtraction(key));
            }
            position = matcher.end();
        }
        return Optional.of(segments.toArray(new VariantPathSegment[0]));
    }

    /** An object field access or an array index access in a VARIANT path. */
    public abstract static class VariantPathSegment {
        private VariantPathSegment() {
        }
    }

    /** An object field access. */
    public static final class ObjectExtraction extends VariantPathSegment {
        private final String key;

        private ObjectExtraction(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }
    }

    /** An array index access. */
    public static final class ArrayExtraction extends VariantPathSegment {
        private final int index;

        private ArrayExtraction(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
    }
}
