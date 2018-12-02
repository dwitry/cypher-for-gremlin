/*
 * Copyright (c) 2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.gremlin.io;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoSerializersV3d0.PSerializer;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.InputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.KryoShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.OutputShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.SerializerShim;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.PeekingInputAdapter;
import org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded.ShadedInputAdapter;
import org.opencypher.gremlin.traversal.CustomPredicate;

public class CypherForGremlinIoRegistry extends AbstractIoRegistry {
    public CypherForGremlinIoRegistry() {
        register(GryoIo.class, P.class, new PredicateSerializer());
    }

    static class PredicateSerializer implements SerializerShim<P> {
        PSerializer delegate = new PSerializer();

        @Override
        public <O extends OutputShim> void write(KryoShim<?, O> kryo, O output, P object) {
            delegate.write(kryo, output, object);
        }

        @Override
        public <I extends InputShim> P read(KryoShim<I, ?> kryo, I input, Class<P> clazz) {
            PeekingInputAdapter peekingInput = new PeekingInputAdapter((ShadedInputAdapter) input);

            String predicate = peekingInput.peekString();

            if (CustomPredicate.NAMES.contains(predicate)) {
                checkState(input.readByte()!=0, "Collection could not be CustomPredicate argument");
                Object value = kryo.readClassAndObject(input);
                return CustomPredicate.byName(predicate, value);
            } else {
                return delegate.read(kryo, (I) peekingInput, clazz);
            }
        }

        private void checkState(boolean condition, String error) {
            if (!condition) {
                throw new IllegalStateException(error);
            }
        }
    }

}
