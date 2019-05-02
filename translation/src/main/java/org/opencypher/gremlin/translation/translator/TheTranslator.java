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
package org.opencypher.gremlin.translation.translator;


import java.util.Collections;
import java.util.Set;
import org.opencypher.gremlin.translation.GremlinBindings;
import org.opencypher.gremlin.translation.GremlinSteps;
import org.opencypher.gremlin.translation.GremlinTokens;

/**
 * Abstraction over the process of building a translation
 * for different targets.
 * <p>
 * Translator instances are not reusable.
 */
public class TheTranslator<T, P extends GremlinTokens> {
    private final GremlinSteps<T, P> steps;
    private final P tokens;
    private final GremlinBindings bindings;
    private final Set<TranslatorFeature> features;
    private final TranslatorFlavor flavor;

    public TheTranslator(GremlinSteps<T, P> steps,
                          P tokens,
                          GremlinBindings bindings,
                          Set<TranslatorFeature> features,
                          TranslatorFlavor flavor) {
        this.steps = steps;
        this.tokens = tokens;
        this.bindings = bindings;
        this.features = features;
        this.flavor = flavor;
    }

    /**
     * Provides access to the traversal DSL.
     *
     * @return traversal DSL
     * @see #tokens()
     * @see #bindings()
     */
    public GremlinSteps<T, P> steps() {
        return steps;
    }

    /**
     * Returns a factory for traversal tokens for use with the traversal DSL.
     *
     * @return factory for traversal tokens
     * @see #steps()
     * @see #bindings()
     */
    public GremlinTokens tokens() {
        return tokens;
    }

    /**
     * Returns a strategy for working with query bindings.
     *
     * @return strategy for query bindings
     * @see #steps()
     * @see #tokens()
     */
    public GremlinBindings bindings() {
        return bindings;
    }

    /**
     * Returns true if a given feature is enabled in this translator.
     *
     * @param feature the feature
     * @return true, if the feature is enabled, false otherwise
     */
    public boolean isEnabled(TranslatorFeature feature) {
        return features.contains(feature);
    }

    /**
     * Returns set of translator features
     *
     * @return set of {@link TranslatorFeature}
     */
    public Set<TranslatorFeature> features() {
        return Collections.unmodifiableSet(features);
    }

    /**
     * Returns the flavor of this translation.
     *
     * @return translation flavor
     */
    public TranslatorFlavor flavor() {
        return flavor;
    }

    /**
     * Creates a translation for the configured target.
     *
     * @return translation
     */
    public T translate() {
        return steps.current();
    }
}