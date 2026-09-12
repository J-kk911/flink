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

import org.apache.flink.table.runtime.functions.VariantPathParser.ArrayExtraction;
import org.apache.flink.table.runtime.functions.VariantPathParser.ObjectExtraction;
import org.apache.flink.table.runtime.functions.VariantPathParser.VariantPathSegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VariantPathParser}. */
class VariantPathParserTest {

    @Test
    void testRootPath() {
        assertThat(VariantPathParser.parse("$"))
                .hasValueSatisfying(segments -> assertThat(segments).isEmpty());
    }

    @ParameterizedTest
    @MethodSource("objectPaths")
    void testObjectField(String path, String expectedKey) {
        assertThat(VariantPathParser.parse(path))
                .hasValueSatisfying(
                        segments ->
                                assertThat(segments)
                                        .singleElement()
                                        .satisfies(segment -> assertObject(segment, expectedKey)));
    }

    private static Stream<Arguments> objectPaths() {
        return Stream.of(
                Arguments.of("$.name", "name"),
                Arguments.of("$['name']", "name"),
                Arguments.of("$[\"name\"]", "name"),
                Arguments.of("$['a.b[0]']", "a.b[0]"),
                Arguments.of("$[\"a.b[0]\"]", "a.b[0]"),
                Arguments.of("$['']", ""),
                Arguments.of("$[\"\"]", ""),
                Arguments.of("$['123']", "123"),
                Arguments.of("$['a\"b']", "a\"b"),
                Arguments.of("$[\"a'b\"]", "a'b"),
                Arguments.of("$['a\\nb']", "a\\nb"),
                Arguments.of("$[\"a\\nb\"]", "a\\nb"),
                Arguments.of("$. first name ", " first name "),
                Arguments.of("$.*", "*"));
    }

    @ParameterizedTest
    @MethodSource("arrayPaths")
    void testArrayIndex(String path, int expectedIndex) {
        assertThat(VariantPathParser.parse(path))
                .hasValueSatisfying(
                        segments ->
                                assertThat(segments)
                                        .singleElement()
                                        .satisfies(segment -> assertArray(segment, expectedIndex)));
    }

    private static Stream<Arguments> arrayPaths() {
        return Stream.of(
                Arguments.of("$[0]", 0),
                Arguments.of("$[123]", 123),
                Arguments.of("$[0002]", 2),
                Arguments.of("$[2147483647]", Integer.MAX_VALUE));
    }

    @Test
    void testMixedPath() {
        assertThat(VariantPathParser.parse("$.items[2]['a.b'][\"name\"]"))
                .hasValueSatisfying(
                        segments ->
                                assertThat(segments)
                                        .satisfiesExactly(
                                                segment -> assertObject(segment, "items"),
                                                segment -> assertArray(segment, 2),
                                                segment -> assertObject(segment, "a.b"),
                                                segment -> assertObject(segment, "name")));
    }

    @Test
    void testNestedArrays() {
        assertThat(VariantPathParser.parse("$[0][1].name"))
                .hasValueSatisfying(
                        segments ->
                                assertThat(segments)
                                        .satisfiesExactly(
                                                segment -> assertArray(segment, 0),
                                                segment -> assertArray(segment, 1),
                                                segment -> assertObject(segment, "name")));
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(
            strings = {
                "name",
                ".name",
                "[0]",
                "$name",
                "$$",
                " $",
                "$ ",
                "$.",
                "$..name",
                "$.name.",
                "$[",
                "$[]",
                "$[-1]",
                "$[+1]",
                "$[1.0]",
                "$[ 0]",
                "$[0 ]",
                "$[2147483648]",
                "$[999999999999999999999999]",
                "$[*]",
                "$[0:2]",
                "$[0,1]",
                "$[?(@.a)]",
                "$['name]",
                "$[\"name]",
                "$['name'",
                "$[\"name\"",
                "$['a\\'b']",
                "$[\"a\\\"b\"]",
                "$[0]junk",
                "$[0] .name",
                "$['name']junk",
                "$.items[2]..name",
                "$.items[2]['name']?"
            })
    void testInvalidPath(String path) {
        assertThat(VariantPathParser.parse(path)).isEmpty();
    }

    private static void assertObject(VariantPathSegment segment, String expectedKey) {
        assertThat(segment)
                .isInstanceOfSatisfying(
                        ObjectExtraction.class,
                        extraction -> assertThat(extraction.getKey()).isEqualTo(expectedKey));
    }

    private static void assertArray(VariantPathSegment segment, int expectedIndex) {
        assertThat(segment)
                .isInstanceOfSatisfying(
                        ArrayExtraction.class,
                        extraction -> assertThat(extraction.getIndex()).isEqualTo(expectedIndex));
    }
}
