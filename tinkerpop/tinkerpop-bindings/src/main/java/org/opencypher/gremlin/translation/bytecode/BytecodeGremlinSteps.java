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
package org.opencypher.gremlin.translation.bytecode;

import static org.opencypher.gremlin.translation.groovy.StringTranslationUtils.apply;

import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.opencypher.gremlin.translation.GremlinSteps;
import org.opencypher.gremlin.translation.traversal.TraversalGremlinTokens;
import org.opencypher.gremlin.traversal.CustomFunction;

@SuppressWarnings("unchecked")
public class BytecodeGremlinSteps implements GremlinSteps<Bytecode, TraversalGremlinTokens> {

    private final Bytecode bytecode;

    public BytecodeGremlinSteps() {
        this.bytecode = new Bytecode();
    }

    @Override
    public Bytecode current() {
        return bytecode.clone();
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> start() {
        return new BytecodeGremlinSteps();
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> V() {
        bytecode.addStep(Symbols.V);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> E() {
        bytecode.addStep(Symbols.E);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> addE(String edgeLabel) {
        bytecode.addStep(Symbols.addE, edgeLabel);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> addV() {
        bytecode.addStep(Symbols.addV);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> addV(String vertexLabel) {
        bytecode.addStep(Symbols.addV, vertexLabel);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> aggregate(String sideEffectKey) {
        bytecode.addStep(Symbols.aggregate, sideEffectKey);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> and(GremlinSteps<Bytecode, TraversalGremlinTokens>... andTraversals) {
        bytecode.addStep(Symbols.and, (Object[]) traversals(andTraversals));
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> as(String stepLabel) {
        bytecode.addStep(Symbols.as, stepLabel);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> barrier() {
        bytecode.addStep(Symbols.barrier);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> bothE(String... edgeLabels) {
        bytecode.addStep(Symbols.bothE, (Object[]) edgeLabels);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> by(GremlinSteps<Bytecode, TraversalGremlinTokens> traversal) {
        bytecode.addStep(Symbols.by, traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> by(GremlinSteps<Bytecode, TraversalGremlinTokens> traversal, TraversalGremlinTokens order) {
        bytecode.addStep(Symbols.by, traversal.current(), order);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> cap(String sideEffectKey) {
        bytecode.addStep(Symbols.cap, sideEffectKey);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> choose(GremlinSteps<Bytecode, TraversalGremlinTokens> choiceTraversal) {
        bytecode.addStep(Symbols.choose, choiceTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> choose(GremlinSteps<Bytecode, TraversalGremlinTokens> traversalPredicate,
                                                                 GremlinSteps<Bytecode, TraversalGremlinTokens> trueChoice) {
        bytecode.addStep(Symbols.choose, traversalPredicate.current(), trueChoice.current());
        return this;
    }


    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> choose(GremlinSteps<Bytecode, TraversalGremlinTokens> traversalPredicate,
                                                                 GremlinSteps<Bytecode, TraversalGremlinTokens> trueChoice,
                                                                 GremlinSteps<Bytecode, TraversalGremlinTokens> falseChoice) {
        bytecode.addStep(Symbols.choose, traversalPredicate.current(), trueChoice.current(), falseChoice.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> choose(TraversalGremlinTokens predicate,
                                                                 GremlinSteps<Bytecode, TraversalGremlinTokens> trueChoice,
                                                                 GremlinSteps<Bytecode, TraversalGremlinTokens> falseChoice) {
        bytecode.addStep(Symbols.choose, predicate, trueChoice.current(), falseChoice.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> choose(TraversalGremlinTokens predicate, GremlinSteps<Bytecode, TraversalGremlinTokens> trueChoice) {
        bytecode.addStep(Symbols.choose, predicate, trueChoice.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> coalesce(GremlinSteps<Bytecode, TraversalGremlinTokens>... coalesceTraversals) {
        bytecode.addStep(Symbols.coalesce, (Object[]) traversals(coalesceTraversals));
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> constant(Object e) {
        bytecode.addStep(Symbols.constant, e);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> count() {
        bytecode.addStep(Symbols.count);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> count(TraversalGremlinTokens scope) {
        bytecode.addStep(Symbols.count, scope);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> dedup(String... dedupLabels) {
        bytecode.addStep(Symbols.dedup, (Object[]) dedupLabels);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> drop() {
        bytecode.addStep(Symbols.drop);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> emit() {
        bytecode.addStep(Symbols.emit);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> emit(GremlinSteps<Bytecode, TraversalGremlinTokens> traversal) {
        bytecode.addStep(Symbols.emit, traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> flatMap(GremlinSteps<Bytecode, TraversalGremlinTokens> traversal) {
        bytecode.addStep(Symbols.flatMap, traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> fold() {
        bytecode.addStep(Symbols.fold);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> from(String fromStepLabel) {
        bytecode.addStep(Symbols.from, fromStepLabel);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> group() {
        bytecode.addStep(Symbols.group);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> has(String propertyKey) {
        bytecode.addStep(Symbols.has, propertyKey);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> has(String propertyKey, TraversalGremlinTokens predicate) {
        bytecode.addStep(Symbols.has, propertyKey, predicate);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> hasKey(String... labels) {
        bytecode.addStep(Symbols.hasKey, (Object[]) labels);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> hasLabel(String... labels) {
        bytecode.addStep(Symbols.hasLabel, (Object[]) labels);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> hasNot(String propertyKey) {
        bytecode.addStep(Symbols.hasNot, propertyKey);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> id() {
        bytecode.addStep(Symbols.id);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> identity() {
        bytecode.addStep(Symbols.identity);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> inE(String... edgeLabels) {
        bytecode.addStep(Symbols.inE, (Object[]) edgeLabels);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> inV() {
        bytecode.addStep(Symbols.inV);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> index() {
        bytecode.addStep(Symbols.index);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> inject(Object... injections) {
        bytecode.addStep(Symbols.inject, (Object[]) injections);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> is(TraversalGremlinTokens predicate) {
        bytecode.addStep(Symbols.is, predicate);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> key() {
        bytecode.addStep(Symbols.key);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> label() {
        bytecode.addStep(Symbols.label);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> limit(long limit) {
        bytecode.addStep(Symbols.limit, limit);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> limit(TraversalGremlinTokens scope, long limit) {
        bytecode.addStep(Symbols.limit, scope, limit);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> local(GremlinSteps<Bytecode, TraversalGremlinTokens> localTraversal) {
        bytecode.addStep(Symbols.local, localTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> loops() {
        bytecode.addStep(Symbols.loops);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> map(CustomFunction function) {
        String lambdaSource = apply(function.getName()) + ".apply(it)";
        Function lambda = Lambda.function(lambdaSource, "gremlin-groovy");
        bytecode.addStep(Symbols.map, lambda);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> map(GremlinSteps<Bytecode, TraversalGremlinTokens> traversal) {
        bytecode.addStep(Symbols.map, traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> math(String expression) {
        bytecode.addStep(Symbols.math, expression);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> max() {
        bytecode.addStep(Symbols.max);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> max(TraversalGremlinTokens scope) {
        bytecode.addStep(Symbols.max, scope);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> mean() {
        bytecode.addStep(Symbols.mean);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> mean(TraversalGremlinTokens scope) {
        bytecode.addStep(Symbols.mean, scope);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> min() {
        bytecode.addStep(Symbols.min);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> min(TraversalGremlinTokens scope) {
        bytecode.addStep(Symbols.min, scope);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> not(GremlinSteps<Bytecode, TraversalGremlinTokens> notTraversal) {
        bytecode.addStep(Symbols.not, notTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> option(Object pickToken, GremlinSteps<Bytecode, TraversalGremlinTokens> traversalOption) {
        bytecode.addStep(Symbols.option, pickToken, traversalOption.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> optional(GremlinSteps<Bytecode, TraversalGremlinTokens> optionalTraversal) {
        bytecode.addStep(Symbols.optional, optionalTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> or(GremlinSteps<Bytecode, TraversalGremlinTokens>... orTraversals) {
        bytecode.addStep(Symbols.or, (Object[]) traversals(orTraversals));
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> order() {
        bytecode.addStep(Symbols.order);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> otherV() {
        bytecode.addStep(Symbols.otherV);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> outE(String... edgeLabels) {
        bytecode.addStep(Symbols.outE, (Object[]) edgeLabels);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> outV() {
        bytecode.addStep(Symbols.outV);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> path() {
        bytecode.addStep(Symbols.path);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> properties(String... propertyKeys) {
        bytecode.addStep(Symbols.properties, (Object[]) propertyKeys);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> property(TraversalGremlinTokens token, Object value) {
        bytecode.addStep(Symbols.property, token, value);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> property(String key, Object value) {
        bytecode.addStep(Symbols.property, key, value);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> property(TraversalGremlinTokens cardinality, String key, Object value) {
        bytecode.addStep(Symbols.property, cardinality, key, value);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> property(String key, GremlinSteps<Bytecode, TraversalGremlinTokens> traversal) {
        bytecode.addStep(Symbols.property, key, traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> property(TraversalGremlinTokens cardinality, String key, GremlinSteps<Bytecode, TraversalGremlinTokens> traversal) {
        bytecode.addStep(Symbols.property, cardinality, key, traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> project(String... keys) {
        bytecode.addStep(Symbols.project, (Object[]) keys);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> range(TraversalGremlinTokens scope, long low, long high) {
        bytecode.addStep(Symbols.range, scope, low, high);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> repeat(GremlinSteps<Bytecode, TraversalGremlinTokens> repeatTraversal) {
        bytecode.addStep(Symbols.repeat, repeatTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> select(final TraversalGremlinTokens pop, String selectKey) {
        bytecode.addStep(Symbols.select, pop, selectKey);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> select(String... selectKeys) {
        bytecode.addStep(Symbols.select, (Object[]) selectKeys);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> select(TraversalGremlinTokens column) {
        bytecode.addStep(Symbols.select, column);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> sideEffect(GremlinSteps<Bytecode, TraversalGremlinTokens> sideEffectTraversal) {
        bytecode.addStep(Symbols.sideEffect, sideEffectTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> simplePath() {
        bytecode.addStep(Symbols.simplePath);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> skip(long skip) {
        bytecode.addStep(Symbols.skip, skip);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> sum() {
        bytecode.addStep(Symbols.sum);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> sum(TraversalGremlinTokens scope) {
        bytecode.addStep(Symbols.sum, scope);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> tail(TraversalGremlinTokens scope, long limit) {
        bytecode.addStep(Symbols.tail, scope, limit);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> times(int maxLoops) {
        bytecode.addStep(Symbols.times, maxLoops);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> to(String toStepLabel) {
        bytecode.addStep(Symbols.to, toStepLabel);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> unfold() {
        bytecode.addStep(Symbols.unfold);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> union(GremlinSteps<Bytecode, TraversalGremlinTokens>... unionTraversals) {
        bytecode.addStep(Symbols.union, (Object[]) traversals(unionTraversals));
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> until(GremlinSteps<Bytecode, TraversalGremlinTokens> untilTraversal) {
        bytecode.addStep(Symbols.until, untilTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> value() {
        bytecode.addStep(Symbols.value);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> valueMap() {
        bytecode.addStep(Symbols.valueMap);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> valueMap(boolean includeTokens) {
        bytecode.addStep(Symbols.valueMap, includeTokens);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> values(String... propertyKeys) {
        bytecode.addStep(Symbols.values, (Object[]) propertyKeys);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> where(GremlinSteps<Bytecode, TraversalGremlinTokens> whereTraversal) {
        bytecode.addStep(Symbols.where, whereTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> where(TraversalGremlinTokens predicate) {
        bytecode.addStep(Symbols.where, predicate);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> with(String name, Object value) {
        bytecode.addStep(Symbols.with, name, value);
        return this;
    }

    @Override
    public GremlinSteps<Bytecode, TraversalGremlinTokens> with(String key) {
        bytecode.addStep(Symbols.with, key);
        return this;
    }

    private static Bytecode[] traversals(GremlinSteps<Bytecode, TraversalGremlinTokens>[] gremlinSteps) {
        return Stream.of(gremlinSteps)
            .map(GremlinSteps::current)
            .toArray(Bytecode[]::new);
    }

    @Override
    public String toString() {
        return bytecode.toString();
    }
}
