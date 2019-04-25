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

import java.util.function.Function;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.opencypher.gremlin.translation.TokensConverter;
import org.opencypher.gremlin.translation.ir.model.Cardinality;
import org.opencypher.gremlin.translation.ir.model.CustomFunction;
import org.opencypher.gremlin.translation.ir.model.Pick;
import org.opencypher.gremlin.translation.ir.model.TraversalOrder;
import org.opencypher.gremlin.traversal.CustomFunctions;

public class TraversalTokensConverter implements TokensConverter<P, Function<Traverser, Object>, Scope, Column, Order, WithOptions, Pop, VertexProperty.Cardinality, TraversalOptionParent.Pick> {
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
    @SuppressWarnings("deprecation")
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

    @Override
    public TraversalOptionParent.Pick convert(Pick pickToken) {
        if (Pick.any == pickToken) {
            return TraversalOptionParent.Pick.any;
        } else {
            return TraversalOptionParent.Pick.none;
        }
    }

    @Override
    public Function<Traverser, Object> convert(CustomFunction function) {
        if (CustomFunction.cypherRound == function) {
            return CustomFunctions.cypherRound();
        } else if (CustomFunction.cypherToString == function) {
            return CustomFunctions.cypherToString();
        } else if (CustomFunction.cypherToBoolean == function) {
            return CustomFunctions.cypherToBoolean();
        } else if (CustomFunction.cypherToInteger == function) {
            return CustomFunctions.cypherToInteger();
        } else if (CustomFunction.cypherToFloat == function) {
            return CustomFunctions.cypherToFloat();
        } else if (CustomFunction.cypherProperties == function) {
            return CustomFunctions.cypherProperties();
        } else if (CustomFunction.cypherContainerIndex == function) {
            return CustomFunctions.cypherContainerIndex();
        } else if (CustomFunction.cypherListSlice == function) {
            return CustomFunctions.cypherListSlice();
        } else if (CustomFunction.cypherPercentileCont == function) {
            return CustomFunctions.cypherPercentileCont();
        } else if (CustomFunction.cypherPercentileDisc == function) {
            return CustomFunctions.cypherPercentileDisc();
        } else if (CustomFunction.cypherSize == function) {
            return CustomFunctions.cypherSize();
        } else if (CustomFunction.cypherPlus == function) {
            return CustomFunctions.cypherPlus();
        } else if (CustomFunction.cypherException == function) {
            return CustomFunctions.cypherException();
        } else if (CustomFunction.cypherSplit == function) {
            return CustomFunctions.cypherSplit();
        } else if (CustomFunction.cypherReverse == function) {
            return CustomFunctions.cypherReverse();
        } else if (CustomFunction.cypherSubstring == function) {
            return CustomFunctions.cypherSubstring();
        } else if (CustomFunction.cypherTrim == function) {
            return CustomFunctions.cypherTrim();
        } else if (CustomFunction.cypherToLower == function) {
            return CustomFunctions.cypherToLower();
        } else if (CustomFunction.cypherToUpper == function) {
            return CustomFunctions.cypherToUpper();
        } else if (CustomFunction.cypherReplace == function) {
            return CustomFunctions.cypherReplace();
        } else if (CustomFunction.cypherCopyProperties == function) {
            return CustomFunctions.cypherCopyProperties();
        } else {
            throw new IllegalStateException("Not implemented");
        }
    }
}
