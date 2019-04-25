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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MathStepAccessor {
    private static final String[] FUNCTIONS = new String[]{
            "abs", "acos", "asin", "atan",
            "cbrt", "ceil", "cos", "cosh",
            "exp",
            "floor",
            "log", "log10", "log2",
            "signum", "sin", "sinh", "sqrt",
            "tan", "tanh"
    };

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\b(?!" +
            String.join("|", FUNCTIONS) + "|([0-9]+))([a-zA-Z_][a-zA-Z0-9_]*)\\b");

    public static final Set<String> getVariables(final String equation) {
        final Matcher matcher = VARIABLE_PATTERN.matcher(equation);
        final Set<String> variables = new LinkedHashSet<>();
        while (matcher.find()) {
            variables.add(matcher.group());
        }
        return variables;
    }
}
