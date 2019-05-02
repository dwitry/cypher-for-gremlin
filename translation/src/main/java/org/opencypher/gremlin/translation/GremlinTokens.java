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
package org.opencypher.gremlin.translation;

/**
 * todo
 */
public interface GremlinTokens<PR, FN, SC, CO, OR, WI, PO, CA, TO> {
    GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> local();

    GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> global();

    GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> values();

    GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> keys();

    GremlinPredicates<PR> predicates();

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> predicate(PR predicate) {
        return new GremlinToken<>(predicate, null, null, null, null, null, null, null, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> function(FN funtion) {
        return new GremlinToken<>(null, funtion, null, null, null, null, null, null, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> scope(SC scope) {
        return new GremlinToken<>(null, null, scope, null, null, null, null, null, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> column(CO column) {
        return new GremlinToken<>(null, null, null, column, null, null, null, null, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> order(OR order) {
        return new GremlinToken<>(null, null, null, null, order, null, null, null, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> with(WI with) {
        return new GremlinToken<>(null, null, null, null, null, with, null, null, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> pop(PO pop) {
        return  new GremlinToken<>(null, null, null, null, null, null, pop, null, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> cardinality(CA cardinality) {
        return new GremlinToken<>(null, null, null, null, null, null, null, cardinality, null);
    }

    default GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> token(TO token) {
        return new GremlinToken<>(null, null, null, null, null, null, null, null, token);
    }

    default PR getPredicate() {throw new IllegalStateException("Should not be called directly");}

    default FN getFunction(){throw new IllegalStateException("Should not be called directly");}

    default SC getScope(){throw new IllegalStateException("Should not be called directly");}

    default CO getColumn(){throw new IllegalStateException("Should not be called directly");}

    default OR getOrder(){throw new IllegalStateException("Should not be called directly");}

    default WI getWith(){throw new IllegalStateException("Should not be called directly");}

    default PO getPop(){throw new IllegalStateException("Should not be called directly");}

    default CA getCardinality(){throw new IllegalStateException("Should not be called directly");}

    default TO getToken(){throw new IllegalStateException("Should not be called directly");}

    class GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> implements GremlinTokens<PR, FN, SC, CO, OR, WI, PO, CA, TO> {
        final PR predicate;
        final FN function;
        final SC scope;
        final CO column;
        final OR order;
        final WI with;
        final PO pop;
        final CA cardinality;
        final TO token;

        public GremlinToken(PR predicate, FN function, SC scope, CO column, OR order, WI with, PO pop, CA cardinality, TO token) {
            this.predicate = predicate;
            this.function = function;
            this.scope = scope;
            this.column = column;
            this.order = order;
            this.with = with;
            this.pop = pop;
            this.cardinality = cardinality;
            this.token = token;
        }

        public PR getPredicate() {
            return check(predicate);
        }

        public FN getFunction() {
            return check(function);
        }

        public SC getScope() {
            return check(scope);
        }

        public CO getColumn() {
            return check(column);
        }

        public OR getOrder() {
            return check(order);
        }

        public WI getWith() {
            return check(with);
        }

        public PO getPop() {
            return check(pop);
        }

        public CA getCardinality() {
            return check(cardinality);
        }

        public TO getToken() {
            return check(token);
        }

        @Override
        public GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> local() {
            throw new IllegalStateException("Should not be called directly");
        }

        @Override
        public GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> global() {
            throw new IllegalStateException("Should not be called directly");
        }

        @Override
        public GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> values() {
            throw new IllegalStateException("Should not be called directly");
        }

        @Override
        public GremlinToken<PR, FN, SC, CO, OR, WI, PO, CA, TO> keys() {
            throw new IllegalStateException("Should not be called directly");
        }

        @Override
        public GremlinPredicates<PR> predicates() {
            throw new IllegalStateException("Should not be called directly");
        }

        private <T> T check(T o) {
            if (o == null) {
                throw new IllegalStateException("Wrong type of token");
            }
            return o;
        }
    }
}
