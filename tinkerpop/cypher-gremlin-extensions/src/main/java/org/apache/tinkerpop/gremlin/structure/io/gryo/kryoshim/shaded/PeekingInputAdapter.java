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
package org.apache.tinkerpop.gremlin.structure.io.gryo.kryoshim.shaded;

import org.apache.tinkerpop.shaded.kryo.io.Input;

public class PeekingInputAdapter extends ShadedInputAdapter {
    private final Input delegate;
    private String peeked;

    public PeekingInputAdapter(ShadedInputAdapter delegate) {
        super(delegate.getShadedInput());
        this.delegate = delegate.getShadedInput();
    }

    public String peekString() {
        peeked = delegate.readString();
        return peeked;
    }

    @Override
    public String readString() {
        String result;
        if (peeked != null) {
            result = peeked;
            peeked = null;
        } else {
            result = delegate.readString();
        }

        return result;
    }
}
