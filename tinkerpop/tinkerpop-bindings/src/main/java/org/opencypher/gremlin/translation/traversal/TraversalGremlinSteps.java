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
package org.opencypher.gremlin.translation.traversal;

import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.opencypher.gremlin.translation.GremlinSteps;
import org.opencypher.gremlin.traversal.CustomFunction;

@SuppressWarnings("unchecked")
public class TraversalGremlinSteps implements GremlinSteps<GraphTraversal, TraversalGremlinTokens> {

    private final GraphTraversal g;

    public TraversalGremlinSteps(GraphTraversal g) {
        this.g = g;
    }

    @Override
    public GraphTraversal current() {
        return g.asAdmin().clone();
    }

    private boolean isStartedOrSubTraversal() {
        boolean hasSteps = g.asAdmin().getSteps().size() > 0;
        boolean isSubTraversal = g.asAdmin().getGraph()
            .filter(graph -> graph instanceof EmptyGraph)
            .isPresent();
        return hasSteps || isSubTraversal;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> start() {
        GraphTraversal g = __.start();
        return new TraversalGremlinSteps(g);
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> V() {
        if (isStartedOrSubTraversal()) {
            g.V();
        } else {
            // Workaround for constructing `GraphStep` with `isStart == true`
            g.asAdmin().getBytecode().addStep(Symbols.V);
            g.asAdmin().addStep(new GraphStep<>(g.asAdmin(), Vertex.class, true));
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> E() {
        if (isStartedOrSubTraversal()) {
            throw new IllegalStateException("Edge graph step can only be at the start of traversal");
        } else {
            // Workaround for constructing `EdgeStep` with `isStart == true`
            g.asAdmin().getBytecode().addStep(Symbols.E);
            g.asAdmin().addStep(new GraphStep<>(g.asAdmin(), Edge.class, true));
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> addE(String edgeLabel) {
        g.addE(edgeLabel);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> addV() {
        if (isStartedOrSubTraversal()) {
            g.addV();
        } else {
            // Workaround for constructing `GraphStep` with `isStart == true`
            g.asAdmin().getBytecode().addStep(Symbols.addV);
            g.asAdmin().addStep(new AddVertexStartStep(g.asAdmin(), (String) null));
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> addV(String vertexLabel) {
        if (isStartedOrSubTraversal()) {
            g.addV(vertexLabel);
        } else {
            // Workaround for constructing `GraphStep` with `isStart == true`
            g.asAdmin().getBytecode().addStep(Symbols.addV, vertexLabel);
            g.asAdmin().addStep(new AddVertexStartStep(g.asAdmin(), vertexLabel));
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> aggregate(String sideEffectKey) {
        g.aggregate(sideEffectKey);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> and(GremlinSteps<GraphTraversal, TraversalGremlinTokens>... andTraversals) {
        g.and(traversals(andTraversals));
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> as(String stepLabel) {
        g.as(stepLabel);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> barrier() {
        g.barrier();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> bothE(String... edgeLabels) {
        g.bothE(edgeLabels);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> by(GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversal, TraversalGremlinTokens order) {
        g.by(traversal.current(), order.getOrder());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> by(GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversal) {
        g.by(traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> cap(String sideEffectKey) {
        g.cap(sideEffectKey);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> choose(final GremlinSteps<GraphTraversal, TraversalGremlinTokens> choiceTraversal) {
        g.choose(choiceTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> choose(final GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversalPredicate,
                                                  GremlinSteps<GraphTraversal, TraversalGremlinTokens> trueChoice) {
        g.choose(traversalPredicate.current(), trueChoice.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> choose(final GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversalPredicate,
                                                  GremlinSteps<GraphTraversal, TraversalGremlinTokens> trueChoice,
                                                  GremlinSteps<GraphTraversal, TraversalGremlinTokens> falseChoice) {
        g.choose(traversalPredicate.current(), trueChoice.current(), falseChoice.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> choose(TraversalGremlinTokens predicate,
                                                  GremlinSteps<GraphTraversal, TraversalGremlinTokens> trueChoice,
                                                  GremlinSteps<GraphTraversal, TraversalGremlinTokens> falseChoice) {
        g.choose(predicate.getPredicate(), trueChoice.current(), falseChoice.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> choose(TraversalGremlinTokens predicate, GremlinSteps<GraphTraversal, TraversalGremlinTokens> trueChoice) {
        g.choose(predicate.getPredicate(), trueChoice.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> coalesce(GremlinSteps<GraphTraversal, TraversalGremlinTokens>... coalesceTraversals) {
        g.coalesce(traversals(coalesceTraversals));
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> constant(Object e) {
        g.constant(e);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> count() {
        g.count();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> count(TraversalGremlinTokens scope) {
        g.count(scope.getScope());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> dedup(String... dedupLabels) {
        g.dedup(dedupLabels);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> drop() {
        g.drop();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> emit() {
        g.emit();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> emit(GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversal) {
        g.emit(traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> flatMap(GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversal) {
        g.flatMap(traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> fold() {
        g.fold();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> from(String fromStepLabel) {
        g.from(fromStepLabel);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> group() {
        g.group();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> has(String propertyKey) {
        g.has(propertyKey);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> has(String propertyKey, TraversalGremlinTokens predicate) {
        g.has(propertyKey, predicate.getPredicate());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> hasKey(String... labels) {
        if (labels.length >= 1) {
            g.hasKey(labels[0], argumentsSlice(labels, 1));
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> hasLabel(String... labels) {
        if (labels.length >= 1) {
            g.hasLabel(labels[0], argumentsSlice(labels, 1));
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> hasNot(String propertyKey) {
        g.hasNot(propertyKey);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> id() {
        g.id();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> identity() {
        g.identity();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> inE(String... edgeLabels) {
        g.inE(edgeLabels);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> inV() {
        g.inV();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> index() {
        g.index();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> inject(Object... injections) {
        g.inject(injections);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> is(TraversalGremlinTokens predicate) {
        g.is(predicate.getPredicate());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> key() {
        g.key();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> label() {
        g.label();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> limit(long limit) {
        g.limit(limit);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> limit(TraversalGremlinTokens scope, long limit) {
        g.limit(scope.getScope(), limit);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> local(GremlinSteps<GraphTraversal, TraversalGremlinTokens> localTraversal) {
        g.local(localTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> loops() {
        g.loops();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> map(CustomFunction function) {
        g.map(function.getImplementation());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> map(GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversal) {
        g.map(traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> math(String expression) {
        g.math(expression);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> max() {
        g.max();
        return this;
    }


    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> max(TraversalGremlinTokens scope) {
        g.max(scope.getScope());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> mean() {
        g.mean();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> mean(TraversalGremlinTokens scope) {
        g.mean(scope.getScope());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> min() {
        g.min();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> min(TraversalGremlinTokens scope) {
        g.min(scope.getScope());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> not(GremlinSteps<GraphTraversal, TraversalGremlinTokens> notTraversal) {
        g.not(notTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> option(Object pickToken, GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversalOption) {
        g.option(pickToken, traversalOption.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> optional(GremlinSteps<GraphTraversal, TraversalGremlinTokens> optionalTraversal) {
        g.optional(optionalTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> or(GremlinSteps<GraphTraversal, TraversalGremlinTokens>... orTraversals) {
        g.or(traversals(orTraversals));
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> order() {
        g.order();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> otherV() {
        g.otherV();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> outE(String... edgeLabels) {
        g.outE(edgeLabels);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> outV() {
        g.outV();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> path() {
        g.path();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> properties(String... propertyKeys) {
        g.properties(propertyKeys);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> property(TraversalGremlinTokens token, Object value) {
        g.property(token.getToken(), value);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> property(String key, Object value) {
        g.property(key, value);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> property(TraversalGremlinTokens cardinality, String key, Object value) {
        g.property(cardinality.getCardinality(), key, value);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> property(String key, GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversal) {
        return property(key, traversal.current());
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> property(TraversalGremlinTokens cardinality, String key, GremlinSteps<GraphTraversal, TraversalGremlinTokens> traversal) {
        g.property(cardinality.getCardinality(), key, traversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> project(String... keys) {
        if (keys.length >= 1) {
            g.project(keys[0], argumentsSlice(keys, 1));
        } else {
            throw new IllegalArgumentException("`project()` step requires keys");
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> range(TraversalGremlinTokens scope, long low, long high) {
        g.range(scope.getScope(), low, high);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> repeat(GremlinSteps<GraphTraversal, TraversalGremlinTokens> repeatTraversal) {
        g.repeat(repeatTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> select(TraversalGremlinTokens pop, String selectKey) {
        g.select(pop.getPop(), selectKey);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> select(String... selectKeys) {
        if (selectKeys.length >= 2) {
            g.select(selectKeys[0], selectKeys[1], argumentsSlice(selectKeys, 2));
        } else if (selectKeys.length == 1) {
            g.select(selectKeys[0]);
        } else {
            throw new IllegalArgumentException("Select step should have arguments");
        }
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> select(TraversalGremlinTokens column) {
        g.select(column.getColumn());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> sideEffect(GremlinSteps<GraphTraversal, TraversalGremlinTokens> sideEffectTraversal) {
        g.sideEffect(sideEffectTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> simplePath() {
        g.simplePath();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> skip(long skip) {
        g.skip(skip);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> sum() {
        g.sum();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> sum(TraversalGremlinTokens scope) {
        g.sum(scope.getScope());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> tail(TraversalGremlinTokens scope, long limit) {
        g.tail(scope.getScope(), limit);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> times(int maxLoops) {
        g.times(maxLoops);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> to(String toStepLabel) {
        g.to(toStepLabel);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> unfold() {
        g.unfold();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> union(GremlinSteps<GraphTraversal, TraversalGremlinTokens>... unionTraversals) {
        g.union(traversals(unionTraversals));
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> until(GremlinSteps<GraphTraversal, TraversalGremlinTokens> untilTraversal) {
        g.until(untilTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> valueMap() {
        g.valueMap();
        return this;
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> valueMap(boolean includeTokens) {
        g.valueMap(includeTokens);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> value() {
        g.value();
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> values(String... propertyKeys) {
        g.values(propertyKeys);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> where(GremlinSteps<GraphTraversal, TraversalGremlinTokens> whereTraversal) {
        g.where(whereTraversal.current());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> where(TraversalGremlinTokens predicate) {
        g.where(predicate.getPredicate());
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> with(String key) {
        g.with(key);
        return this;
    }

    @Override
    public GremlinSteps<GraphTraversal, TraversalGremlinTokens> with(String name, Object value) {
        g.with(name, value);
        return this;
    }

    private static String[] argumentsSlice(String[] arguments, int start) {
        String[] dest = new String[arguments.length - start];
        System.arraycopy(arguments, start, dest, 0, arguments.length - start);
        return dest;
    }

    private static GraphTraversal[] traversals(GremlinSteps<GraphTraversal, TraversalGremlinTokens>[] gremlinSteps) {
        return Stream.of(gremlinSteps)
            .map(GremlinSteps::current)
            .toArray(GraphTraversal[]::new);
    }

    @Override
    public String toString() {
        return g.asAdmin().getBytecode().toString();
    }
}
