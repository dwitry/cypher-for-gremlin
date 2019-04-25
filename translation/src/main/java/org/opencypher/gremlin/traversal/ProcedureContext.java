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
package org.opencypher.gremlin.traversal;


import java.util.Collection;
import java.util.function.Function;
import org.opencypher.gremlin.extension.CypherProcedureSignature;

public interface ProcedureContext {
    final class LazyHolder {
        private static final ProcedureContext GLOBAL = empty();
    }

    static ProcedureContext global() {
        return LazyHolder.GLOBAL;
    }

    static ProcedureContext empty() {
        return new ProcedureContext() {
            @Override
            public Function<Collection<?>, Object> procedureCall(String name) {
                throw new IllegalArgumentException("Procedure not found: " + name);
            }

            @Override
            public CypherProcedureSignature findOrThrow(String name) {
                throw new IllegalArgumentException("Procedure not found: " + name);
            }
        };
    }

    Function<Collection<?>, Object> procedureCall(String name);

    CypherProcedureSignature findOrThrow(String name);
}
