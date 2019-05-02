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
package org.opencypher.gremlin.translation.groovy;

import static org.opencypher.gremlin.translation.groovy.StringTranslationUtils.apply;
import static org.opencypher.gremlin.translation.groovy.StringTranslationUtils.chain;

import java.util.stream.Stream;
import org.opencypher.gremlin.translation.GremlinSteps;
import org.opencypher.gremlin.translation.traversal.TraversalGremlinTokens;
import org.opencypher.gremlin.traversal.CustomFunction;

public class GroovyGremlinSteps implements GremlinSteps<String, TraversalGremlinTokens> {

    private final StringBuilder g;

    public GroovyGremlinSteps() {
        this("g");
    }

    protected GroovyGremlinSteps(String start) {
        g = new StringBuilder(start);
    }

    @Override
    public String toString() {
        return g.toString();
    }

    @Override
    public String current() {
        return g.toString();
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> start() {
        return new GroovyGremlinSteps("__");
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> V() {
        g.append(chain("V"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> E() {
        g.append(chain("E"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> addE(String edgeLabel) {
        g.append(chain("addE", edgeLabel));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> addV() {
        g.append(chain("addV"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> addV(String vertexLabel) {
        g.append(chain("addV", vertexLabel));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> aggregate(String sideEffectKey) {
        g.append(chain("aggregate", sideEffectKey));
        return this;
    }

    @SafeVarargs
    @Override
    public final GremlinSteps<String, TraversalGremlinTokens> and(GremlinSteps<String, TraversalGremlinTokens>... andTraversals) {
        g.append(chain("and", traversals(andTraversals)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> as(String stepLabel) {
        g.append(chain("as", stepLabel));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> barrier() {
        g.append(chain("barrier"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> bothE(String... edgeLabels) {
        g.append(chain("bothE", (Object[]) edgeLabels));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> by(GremlinSteps<String, TraversalGremlinTokens> traversal) {
        g.append(chain("by", traversal(traversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> by(GremlinSteps<String, TraversalGremlinTokens> traversal, TraversalGremlinTokens order) {
        g.append(chain("by", traversal(traversal), order));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> cap(String sideEffectKey) {
        g.append(chain("cap", sideEffectKey));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> choose(GremlinSteps<String, TraversalGremlinTokens> choiceTraversal) {
        g.append(chain("choose", traversal(choiceTraversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> choose(GremlinSteps<String, TraversalGremlinTokens> predicate,
                                                        GremlinSteps<String, TraversalGremlinTokens> trueChoice) {
        g.append(chain("choose", traversal(predicate), traversal(trueChoice)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> choose(GremlinSteps<String, TraversalGremlinTokens> predicate,
                                                        GremlinSteps<String, TraversalGremlinTokens> trueChoice,
                                                        GremlinSteps<String, TraversalGremlinTokens> falseChoice) {
        g.append(chain("choose", traversal(predicate), traversal(trueChoice), traversal(falseChoice)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> choose(TraversalGremlinTokens predicate,
                                                        GremlinSteps<String, TraversalGremlinTokens> trueChoice,
                                                        GremlinSteps<String, TraversalGremlinTokens> falseChoice) {
        g.append(chain("choose", predicate, traversal(trueChoice), traversal(falseChoice)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> choose(TraversalGremlinTokens predicate,
                                                        GremlinSteps<String, TraversalGremlinTokens> trueChoice) {
        g.append(chain("choose", predicate, traversal(trueChoice)));
        return this;
    }

    @SafeVarargs
    @Override
    public final GremlinSteps<String, TraversalGremlinTokens> coalesce(GremlinSteps<String, TraversalGremlinTokens>... coalesceTraversals) {
        g.append(chain("coalesce", traversals(coalesceTraversals)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> constant(Object e) {
        g.append(chain("constant", e));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> count() {
        g.append(chain("count"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> count(TraversalGremlinTokens scope) {
        g.append(chain("count", scope));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> dedup(String... dedupLabels) {
        g.append(chain("dedup", (Object[]) dedupLabels));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> drop() {
        g.append(chain("drop"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> emit() {
        g.append(chain("emit"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> emit(GremlinSteps<String, TraversalGremlinTokens> traversal) {
        g.append(chain("emit", traversal(traversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> flatMap(GremlinSteps<String, TraversalGremlinTokens> traversal) {
        g.append(chain("flatMap", traversal(traversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> fold() {
        g.append(chain("fold"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> from(String fromStepLabel) {
        g.append(chain("from", fromStepLabel));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> group() {
        g.append(chain("group"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> has(String propertyKey) {
        g.append(chain("has", propertyKey));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> has(String propertyKey, TraversalGremlinTokens predicate) {
        g.append(chain("has", propertyKey, predicate));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> hasKey(String... labels) {
        g.append(chain("hasKey", (Object[]) labels));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> hasLabel(String... labels) {
        g.append(chain("hasLabel", (Object[]) labels));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> hasNot(String propertyKey) {
        g.append(chain("hasNot", propertyKey));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> id() {
        g.append(chain("id"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> identity() {
        g.append(chain("identity"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> inE(String... edgeLabels) {
        g.append(chain("inE", (Object[]) edgeLabels));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> index() {
        g.append(chain("index"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> inV() {
        g.append(chain("inV"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> inject(Object... injections) {
        g.append(chain("inject", injections));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> is(TraversalGremlinTokens predicate) {
        g.append(chain("is", predicate));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> key() {
        g.append(chain("key"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> label() {
        g.append(chain("label"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> limit(long limit) {
        g.append(chain("limit", limit));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> limit(TraversalGremlinTokens scope, long limit) {
        g.append(chain("limit", scope, limit));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> local(GremlinSteps<String, TraversalGremlinTokens> localTraversal) {
        g.append(chain("local", traversal(localTraversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> loops() {
        g.append(chain("loops"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> map(CustomFunction function) {
        g.append(chain(
            "map",
            Verbatim.of(apply(function.getName()))
        ));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> map(GremlinSteps<String, TraversalGremlinTokens> traversal) {
        g.append(chain("map", traversal(traversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> math(String expression) {
        g.append(chain("math", expression));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> max() {
        g.append(chain("max"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> max(TraversalGremlinTokens scope) {
        g.append(chain("max", scope));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> mean() {
        g.append(chain("mean"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> mean(TraversalGremlinTokens scope) {
        g.append(chain("mean", scope));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> min() {
        g.append(chain("min"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> min(TraversalGremlinTokens scope) {
        g.append(chain("min", scope));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> not(GremlinSteps<String, TraversalGremlinTokens> notTraversal) {
        g.append(chain("not", traversal(notTraversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> option(Object pickToken, GremlinSteps<String, TraversalGremlinTokens> traversalOption) {
        g.append(chain("option", pickToken, traversal(traversalOption)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> optional(GremlinSteps<String, TraversalGremlinTokens> optionalTraversal) {
        g.append(chain("optional", traversal(optionalTraversal)));
        return this;
    }

    @SafeVarargs
    @Override
    public final GremlinSteps<String, TraversalGremlinTokens> or(GremlinSteps<String, TraversalGremlinTokens>... orTraversals) {
        g.append(chain("or", traversals(orTraversals)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> order() {
        g.append(chain("order"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> otherV() {
        g.append(chain("otherV"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> outE(String... edgeLabels) {
        g.append(chain("outE", (Object[]) edgeLabels));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> outV() {
        g.append(chain("outV"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> path() {
        g.append(chain("path"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> properties(String... propertyKeys) {
        g.append(chain("properties", (Object[]) propertyKeys));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> property(TraversalGremlinTokens token, Object value) {
        g.append(chain("property", token, value));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> property(String key, Object value) {
        g.append(chain("property", key, value));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> property(TraversalGremlinTokens cardinality, String key, Object value) {
        g.append(chain("property", cardinality, key, value));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> property(String key, GremlinSteps<String, TraversalGremlinTokens> traversal) {
        g.append(chain("property", key, traversal(traversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> property(TraversalGremlinTokens cardinality, String key, GremlinSteps<String, TraversalGremlinTokens> traversal) {
        g.append(chain("property", cardinality, key, traversal(traversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> project(String... keys) {
        g.append(chain("project", (Object[]) keys));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> range(TraversalGremlinTokens scope, long low, long high) {
        g.append(chain("range", scope, low, high));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> repeat(GremlinSteps<String, TraversalGremlinTokens> repeatTraversal) {
        g.append(chain("repeat", traversal(repeatTraversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> select(final TraversalGremlinTokens pop, String selectKey) {
        g.append(chain("select", pop, selectKey));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> select(String... selectKeys) {
        g.append(chain("select", (Object[]) selectKeys));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> select(TraversalGremlinTokens column) {
        g.append(chain("select", column));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> sideEffect(GremlinSteps<String, TraversalGremlinTokens> sideEffectTraversal) {
        g.append(chain("sideEffect", traversal(sideEffectTraversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> simplePath() {
        g.append(chain("simplePath"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> skip(long skip) {
        g.append(chain("skip", skip));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> sum() {
        g.append(chain("sum"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> sum(TraversalGremlinTokens scope) {
        g.append(chain("sum", scope));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> tail(TraversalGremlinTokens scope, long limit) {
        g.append(chain("tail", scope, limit));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> times(int maxLoops) {
        g.append(chain("times", maxLoops));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> to(String toStepLabel) {
        g.append(chain("to", toStepLabel));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> unfold() {
        g.append(chain("unfold"));
        return this;
    }

    @SafeVarargs
    @Override
    public final GremlinSteps<String, TraversalGremlinTokens> union(GremlinSteps<String, TraversalGremlinTokens>... unionTraversals) {
        g.append(chain("union", traversals(unionTraversals)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> until(GremlinSteps<String, TraversalGremlinTokens> untilTraversal) {
        g.append(chain("until", traversal(untilTraversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> value() {
        g.append(chain("value"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> valueMap() {
        g.append(chain("valueMap"));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> valueMap(boolean includeTokens) {
        g.append(chain("valueMap", includeTokens));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> values(String... propertyKeys) {
        g.append(chain("values", (Object[]) propertyKeys));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> where(GremlinSteps<String, TraversalGremlinTokens> whereTraversal) {
        g.append(chain("where", traversal(whereTraversal)));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> where(TraversalGremlinTokens predicate) {
        g.append(chain("where", predicate));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> with(String key) {
        g.append(chain("with", key));
        return this;
    }

    @Override
    public GremlinSteps<String, TraversalGremlinTokens> with(String name, Object value) {
        g.append(chain("with", name, value));
        return this;
    }

    private static Object traversal(GremlinSteps<String, TraversalGremlinTokens> gremlinStep) {
        return Verbatim.of(gremlinStep.current());
    }

    private static Object[] traversals(GremlinSteps<String, TraversalGremlinTokens>[] gremlinSteps) {
        return Stream.of(gremlinSteps)
            .map(GroovyGremlinSteps::traversal)
            .toArray(Object[]::new);
    }
}
