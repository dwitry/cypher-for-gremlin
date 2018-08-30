/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

// Generates the modern graph into an "empty" Neo4jGraph via LifeCycleHook.
// Note that the name of the key in the "global" map is unimportant.
globals << [hook : [
    onStartUp: { ctx ->
        ctx.logger.info("Loading 'modern' graph data.")
        graph.tx().open()

        def marko = graph.addVertex(T.label, 'person', 'name', 'marko', 'age', 29)
        def vadas = graph.addVertex(T.label, 'person', 'name', 'vadas', 'age', 27)
        def lop = graph.addVertex(T.label, 'software', 'name', 'lop', 'lang', 'java')
        def josh = graph.addVertex(T.label, 'person', 'name', 'josh', 'age', 32)
        def ripple = graph.addVertex(T.label, 'software', 'name', 'ripple', 'lang', 'java')
        def peter = graph.addVertex(T.label, 'person', 'name', 'peter', 'age', 35)

        marko.addEdge('knows', vadas, 'weight', 0.5d)
        marko.addEdge('knows', josh, 'weight', 1.0d)
        marko.addEdge('created', lop, 'weight', 0.4d)
        josh.addEdge('created', ripple,  'weight', 1.0d)
        josh.addEdge('created', lop,  'weight', 0.4d)
        peter.addEdge('created', lop,  'weight', 0.2d)

        graph.tx().commit()
    }
] as LifeCycleHook]

// define the default TraversalSource to bind queries to - this one will be named "g".
globals << [g : graph.traversal()]
