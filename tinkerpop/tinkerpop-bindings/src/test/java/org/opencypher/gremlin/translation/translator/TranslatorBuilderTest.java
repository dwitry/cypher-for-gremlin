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
package org.opencypher.gremlin.translation.translator;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.ir.rewrite.CosmosDbFlavor$;
import org.opencypher.gremlin.translation.ir.rewrite.CustomFunctionFallback$;
import org.opencypher.gremlin.translation.ir.rewrite.Gremlin33xFlavor$;
import org.opencypher.gremlin.translation.ir.rewrite.NeptuneFlavor$;

public class TranslatorBuilderTest {
    @Test
    public void enableCypherExtensions() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().enableCypherExtensions().build();
        String steps = CypherAst.parse("MATCH (a) DELETE a").buildTranslation(dslBuilder);
        assertThat(steps).contains("cypherException()");
    }

    @Test
    public void disableCypherExtensions() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build();
        String steps = CypherAst.parse("MATCH (a) DELETE a").buildTranslation(dslBuilder);
        assertThat(steps).contains("bothE().path().from('Cannot delete node, because it still has relationships. To delete this node, you must first delete its relationships.')");
    }

    @Test
    public void cosmosDb() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("cosmosdb");

        assertThatThrownBy(
          () ->
            CypherAst.parse("RETURN toupper('test')")
              .buildTranslation(dslBuilder))
          .hasMessageContaining("Custom functions and predicates are not supported: cypherToUpper");

        String steps = CypherAst.parse("MATCH (n) RETURN n.name")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(CosmosDbFlavor$.MODULE$)).isTrue();
        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
        assertThat(steps).contains("properties().hasKey('name')");
        assertThat(steps).doesNotContain("values('name')");
    }

    @Test
    public void cosmosDbExtensions() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("cosmosdb+cfog_server_extensions");

        String steps = CypherAst.parse("MATCH (n) RETURN toupper(n.name)")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(CosmosDbFlavor$.MODULE$)).isTrue();
        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
        assertThat(steps).contains("cypherToUpper()");
        assertThat(steps).contains("properties().hasKey('name')");
        assertThat(steps).doesNotContain("values('name')");
    }

    @Test
    public void neptuneDb() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("neptune");

        assertThatThrownBy(
          () ->
            CypherAst.parse("RETURN toupper('test')")
              .buildTranslation(dslBuilder))
          .hasMessageContaining("Custom functions and predicates are not supported: cypherToUpper");

        String steps = CypherAst.parse("MATCH (n) WHERE n.age=$age RETURN count(n)", parameterMap("age", 25L))
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(NeptuneFlavor$.MODULE$)).isTrue();
        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
        assertThat(steps).contains("constant(25)"); // inline parameters
        assertThat(steps).contains("count().barrier()");
    }

    @Test
    public void neptuneDbExtensions() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("neptune+cfog_server_extensions");

        String steps = CypherAst.parse("MATCH (n) WHERE toupper(n.age)=$age RETURN count(n)", parameterMap("age", 25L))
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(NeptuneFlavor$.MODULE$)).isTrue();
        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
        assertThat(steps).contains("cypherToUpper()");
        assertThat(steps).contains("constant(25)"); // inline parameters
        assertThat(steps).contains("count().barrier()");
    }

    @Test
    public void gremlin33x() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("gremlin33x");

        assertThatThrownBy(
          () ->
            CypherAst.parse("RETURN toupper('test')")
              .buildTranslation(dslBuilder))
          .hasMessageContaining("Custom functions and predicates are not supported: cypherToUpper");

        String steps = CypherAst.parse("MATCH (n) RETURN n.name ORDER BY n.name")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isTrue();
        assertThat(dslBuilder.flavor().rewriters().contains(Gremlin33xFlavor$.MODULE$)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
        assertThat(steps).contains("by(__.select('n.name'), incr)");
    }

    @Test
    public void vanilla() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("vanilla");

        assertThatThrownBy(
          () ->
            CypherAst.parse("RETURN toupper('test')")
              .buildTranslation(dslBuilder))
          .hasMessageContaining("Custom functions and predicates are not supported: cypherToUpper");

        String steps = CypherAst.parse("MATCH (n) RETURN n.name")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
    }

    @Test
    public void empty() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("");

        assertThatThrownBy(
          () ->
            CypherAst.parse("RETURN toupper('test')")
              .buildTranslation(dslBuilder))
          .hasMessageContaining("Custom functions and predicates are not supported: cypherToUpper");

        String steps = CypherAst.parse("MATCH (n) RETURN n.name")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
    }

    @Test
    public void nullParam() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build((String) null);

        assertThatThrownBy(
          () ->
            CypherAst.parse("RETURN toupper('test')")
              .buildTranslation(dslBuilder))
          .hasMessageContaining("Custom functions and predicates are not supported: cypherToUpper");

        String steps = CypherAst.parse("MATCH (n) RETURN n.name")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
    }

    @Test
    public void gremlinExtensions() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("gremlin+cfog_server_extensions");

        String steps = CypherAst.parse("MATCH (n) RETURN toupper(n.name)")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.flavor().rewriters().contains(CustomFunctionFallback$.MODULE$)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();
    }

    @Test
    public void gremlinFunction() {
        Translator<String, GroovyPredicate> dslBuilder = createBuilder().build("gremlin+experimental_gremlin_function");

        String steps = CypherAst.parse("RETURN gremlin(\"g.V().hasLabel(\'inject\')\")")
            .buildTranslation(dslBuilder);

        assertThat(dslBuilder.isEnabled(TranslatorFeature.EXPERIMENTAL_GREMLIN_FUNCTION)).isTrue();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.CYPHER_EXTENSIONS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.MULTIPLE_LABELS)).isFalse();
        assertThat(dslBuilder.isEnabled(TranslatorFeature.RETURN_GREMLIN_ELEMENTS)).isFalse();

        assertThat(steps).contains("V().hasLabel('inject')");
    }

    private Translator.ParametrizedFlavorBuilder<String, GroovyPredicate> createBuilder() {
        return Translator.builder().gremlinGroovy();
    }

    public static Map<String, Object> parameterMap(Object... parameters) {
        HashMap<String, Object> result = new HashMap<>();
        for (int i = 0; i < parameters.length; i += 2) {
            result.put(String.valueOf(parameters[i]), parameters[i + 1]);
        }
        return result;
    }
}
