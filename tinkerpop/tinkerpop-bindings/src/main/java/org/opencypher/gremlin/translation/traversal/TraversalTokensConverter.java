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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.opencypher.gremlin.translation.TokensConverter;
import org.opencypher.gremlin.translation.ir.model.Cardinality;
import org.opencypher.gremlin.translation.ir.model.TraversalOrder;
import org.opencypher.gremlin.traversal.CustomFunction;

public class TraversalTokensConverter implements TokensConverter<P, CustomFunction, Scope, Column, Order, WithOptions, Pop, VertexProperty.Cardinality, T> {
    @Override
    public Scope convert(org.opencypher.gremlin.translation.ir.model.Scope scope) {
        if (org.opencypher.gremlin.translation.ir.model.Scope.local == scope) {
            return Scope.local;
        } else {
            return Scope.global;
        }
    }

    @Override
    public Pop convert(org.opencypher.gremlin.translation.ir.model.Pop pop) {
        if (org.opencypher.gremlin.translation.ir.model.Pop.all == pop) {
            return org.apache.tinkerpop.gremlin.process.traversal.Pop.all;
        } else {
            throw new IllegalStateException("Not implemented");
        }
    }

    @Override
    public VertexProperty.Cardinality convert(Cardinality cardinality) {
        if (Cardinality.single == cardinality) {
            return VertexProperty.Cardinality.single;
        } else {
            throw new IllegalStateException("Not implemented");
        }
    }

    @Override
    public Column convert(org.opencypher.gremlin.translation.ir.model.Column column) {
        if (org.opencypher.gremlin.translation.ir.model.Column.keys == column) {
            return Column.keys;
        } else {
            return Column.values;
        }
    }

    @Override
    public Order convert(TraversalOrder order) {
        if (TraversalOrder.asc == order) {
            return Order.asc;
        } else if (TraversalOrder.desc == order) {
            return Order.desc;
        } else if (TraversalOrder.incr == order) {
            return Order.incr;
        } else if (TraversalOrder.decr == order) {
            return Order.decr;
        } else {
            throw new IllegalStateException("Not implemented");
        }
    }

}
