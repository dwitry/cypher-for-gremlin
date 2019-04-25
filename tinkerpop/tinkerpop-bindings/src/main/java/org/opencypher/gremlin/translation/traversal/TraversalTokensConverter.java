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

import java.util.Collection;
import java.util.function.Function;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.opencypher.gremlin.translation.TokensConverter;
import org.opencypher.gremlin.translation.ir.model.Cardinality;
import org.opencypher.gremlin.translation.ir.model.CustomFunction;
import org.opencypher.gremlin.translation.ir.model.Pick;
import org.opencypher.gremlin.translation.ir.model.TraversalOrder;
import org.opencypher.gremlin.traversal.CustomFunctions;

public class TraversalTokensConverter implements TokensConverter<Function<Traverser, Object>, Scope, Column, Order, Pop, VertexProperty.Cardinality, TraversalOptionParent.Pick> {
    @Override
    public Scope convert(org.opencypher.gremlin.translation.ir.model.Scope scope) {
        switch (scope) {
            case global:
                return Scope.global;
            case local:
                return Scope.local;
            default:
                return unsupported(scope);
        }
    }

    @Override
    public Pop convert(org.opencypher.gremlin.translation.ir.model.Pop pop) {
        switch (pop) {
            case all:
                return Pop.all;
            case first:
                return Pop.first;
            default:
                return unsupported(pop);
        }
    }

    @Override
    public VertexProperty.Cardinality convert(Cardinality cardinality) {
        switch (cardinality) {
            case single:
                return VertexProperty.Cardinality.single;
            case list:
                return VertexProperty.Cardinality.list;
            default:
                return unsupported(cardinality);
        }
    }

    @Override
    public Column convert(org.opencypher.gremlin.translation.ir.model.Column column) {
        switch (column) {
            case values:
                return Column.values;
            case keys:
                return Column.keys;
            default:
                return unsupported(column);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public Order convert(TraversalOrder order) {
        switch (order) {
            case asc:
                return Order.asc;
            case desc:
                return Order.desc;
            case incr:
                return Order.incr;
            case decr:
                return Order.decr;
            default:
                return unsupported(order);
        }
    }

    @Override
    public TraversalOptionParent.Pick convert(Pick pickToken) {
        switch (pickToken) {
            case any:
                return TraversalOptionParent.Pick.any;
            case none:
                return TraversalOptionParent.Pick.none;
            default:
                return unsupported(pickToken);
        }
    }

    @Override
    public Function<Traverser, Object> convert(CustomFunction function) {
        switch (function) {
            case cypherRound:
                return CustomFunctions.cypherRound();
            case cypherToString:
                return CustomFunctions.cypherToString();
            case cypherToBoolean:
                return CustomFunctions.cypherToBoolean();
            case cypherToInteger:
                return CustomFunctions.cypherToInteger();
            case cypherToFloat:
                return CustomFunctions.cypherToFloat();
            case cypherProperties:
                return CustomFunctions.cypherProperties();
            case cypherContainerIndex:
                return CustomFunctions.cypherContainerIndex();
            case cypherListSlice:
                return CustomFunctions.cypherListSlice();
            case cypherPercentileCont:
                return CustomFunctions.cypherPercentileCont();
            case cypherPercentileDisc:
                return CustomFunctions.cypherPercentileDisc();
            case cypherSize:
                return CustomFunctions.cypherSize();
            case cypherPlus:
                return CustomFunctions.cypherPlus();
            case cypherException:
                return CustomFunctions.cypherException();
            case cypherSplit:
                return CustomFunctions.cypherSplit();
            case cypherReverse:
                return CustomFunctions.cypherReverse();
            case cypherSubstring:
                return CustomFunctions.cypherSubstring();
            case cypherTrim:
                return CustomFunctions.cypherTrim();
            case cypherToLower:
                return CustomFunctions.cypherToLower();
            case cypherToUpper:
                return CustomFunctions.cypherToUpper();
            case cypherReplace:
                return CustomFunctions.cypherReplace();
            case cypherCopyProperties:
                return CustomFunctions.cypherCopyProperties();
            default:
                return unsupported(function);
        }
    }

    @Override
    public Function<Traverser, Object> convert(Function<Collection<?>, Object> function) {
        return traverser -> {
            Collection<?> arguments = (Collection<?>) traverser.get();
            return function.apply(arguments);
        };
    }

    private <T> T unsupported(Object s) {
        throw new IllegalStateException("Not supported " + s);
    }
}
