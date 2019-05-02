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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.opencypher.gremlin.translation.GremlinPredicates;
import org.opencypher.gremlin.translation.GremlinTokens;
import org.opencypher.gremlin.translation.ir.model.WithOptions;
import org.opencypher.gremlin.traversal.CustomFunction;

public class TraversalGremlinTokens implements GremlinTokens<P, CustomFunction, Scope, Column, Order, WithOptions, Pop, VertexProperty.Cardinality, T> {


    @Override
    public GremlinToken<P, CustomFunction, Scope, Column, Order, WithOptions,  Pop, VertexProperty.Cardinality, T> predicate(P predicate) {
        return null;
    }

    @Override
    public GremlinToken<P, CustomFunction, Scope, Column, Order, WithOptions,  Pop, VertexProperty.Cardinality, T> function(CustomFunction function) {
        return null;
    }

    @Override
    public GremlinToken<P, CustomFunction, Scope, Column, Order, WithOptions,  Pop, VertexProperty.Cardinality, T> local() {
        return scope(Scope.local);
    }

    @Override
    public GremlinToken<P, CustomFunction, Scope, Column, Order, WithOptions,  Pop, VertexProperty.Cardinality, T> global() {
        return scope(Scope.global);
    }

    @Override
    public GremlinToken<P, CustomFunction, Scope, Column, Order, WithOptions,  Pop, VertexProperty.Cardinality, T> values() {
        return column(Column.values);
    }

    @Override
    public GremlinToken<P, CustomFunction, Scope, Column, Order, WithOptions,  Pop, VertexProperty.Cardinality, T> keys() {
        return column(Column.keys);
    }

    @Override
    public GremlinPredicates<P> predicates() {
        return new TraversalGremlinPredicates();
    }
}
