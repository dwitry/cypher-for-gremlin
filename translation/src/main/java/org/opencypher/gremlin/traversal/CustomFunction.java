/*
 * Copyright (c) 2018-2019 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.gremlin.traversal;

import java.util.Objects;
import java.util.function.Function;

public class CustomFunction {
    private final String name;
    private final Function implementation;

    CustomFunction(String name, Function implementation) {
        this.name = name;
        this.implementation = implementation;
    }

    public String getName() {
        return name;
    }

    public Function getImplementation() {
        return implementation;
    }

    public static CustomFunction cypherRound() {
        return new CustomFunction(
            "cypherRound",
            null //CustomFunctions.cypherRound()
        );
    }

    public static CustomFunction cypherToString() {
        return new CustomFunction(
            "cypherToString",
            null // CustomFunctions.cypherToString()
        );
    }

    public static CustomFunction cypherToBoolean() {
        return new CustomFunction(
            "cypherToBoolean",
            null //CustomFunctions.cypherToBoolean()
        );
    }

    public static CustomFunction cypherToInteger() {
        return new CustomFunction(
            "cypherToInteger",
            null //CustomFunctions.cypherToInteger()
        );
    }

    public static CustomFunction cypherToFloat() {
        return new CustomFunction(
            "cypherToFloat",
            null //CustomFunctions.cypherToFloat()
        );
    }

    public static CustomFunction cypherProperties() {
        return new CustomFunction(
            "cypherProperties",
            null //CustomFunctions.cypherProperties()
        );
    }

    public static CustomFunction cypherContainerIndex() {
        return new CustomFunction(
            "cypherContainerIndex",
            null //CustomFunctions.cypherContainerIndex()
        );
    }

    public static CustomFunction cypherListSlice() {
        return new CustomFunction(
            "cypherListSlice",
            null //CustomFunctions.cypherListSlice()
        );
    }

    public static CustomFunction cypherPercentileCont() {
        return new CustomFunction(
            "cypherPercentileCont",
            null //CustomFunctions.cypherPercentileCont()
        );
    }

    public static CustomFunction cypherPercentileDisc() {
        return new CustomFunction(
            "cypherPercentileDisc",
            null //CustomFunctions.cypherPercentileDisc()
        );
    }

    public static CustomFunction cypherSize() {
        return new CustomFunction(
            "cypherSize",
            null //CustomFunctions.cypherSize()
        );
    }

    public static CustomFunction cypherPlus() {
        return new CustomFunction(
            "cypherPlus",
            null //CustomFunctions.cypherPlus()
        );
    }

    public static CustomFunction cypherException() {
        return new CustomFunction(
            "cypherException",
            null //CustomFunctions.cypherException()
        );
    }

    public static CustomFunction cypherSplit() {
        return new CustomFunction(
            "cypherSplit",
            null // CustomFunctions.cypherSplit()
        );
    }

    public static CustomFunction cypherReverse() {
        return new CustomFunction(
            "cypherReverse",
            null // CustomFunctions.cypherReverse()
        );
    }

    public static CustomFunction cypherSubstring() {
        return new CustomFunction(
            "cypherSubstring",
            null // CustomFunctions.cypherSubstring()
        );
    }


    public static CustomFunction cypherTrim() {
        return new CustomFunction(
            "cypherTrim",
            null //CustomFunctions.cypherTrim()
        );
    }

    public static CustomFunction cypherToLower() {
        return new CustomFunction(
            "cypherToLower",
            null //CustomFunctions.cypherToLower()
        );
    }

    public static CustomFunction cypherToUpper() {
        return new CustomFunction(
            "cypherToUpper",
            null //CustomFunctions.cypherToUpper()
        );
    }

    public static CustomFunction cypherReplace() {
        return new CustomFunction(
            "cypherReplace",
            null //CustomFunctions.cypherReplace()
        );
    }

    public static CustomFunction cypherCopyProperties() {
        return new CustomFunction(
            "cypherCopyProperties",
            null //CustomFunctions.cypherCopyProperties()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CustomFunction)) return false;
        CustomFunction that = (CustomFunction) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
