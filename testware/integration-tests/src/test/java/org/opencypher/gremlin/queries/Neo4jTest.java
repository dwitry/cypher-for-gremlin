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
package org.opencypher.gremlin.queries;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencypher.gremlin.rules.GremlinServerExternalResource;
import org.opencypher.gremlin.server.EmbeddedGremlinServerFactory;

public class Neo4jTest {
    @ClassRule
    public static final GremlinServerExternalResource gremlinServer =
        new GremlinServerExternalResource(EmbeddedGremlinServerFactory::neo4j);

    private List<Map<String, Object>> submitAndGet(String cypher) {
        return gremlinServer.cypherGremlinClient().submit(cypher).all();
    }

    @Test
    public void test() throws Exception {
        //todo cleanup `/tmp/neo4j`

        List<Map<String, Object>> count = submitAndGet("MATCH (n) RETURN count(n)");

        assertThat(count)
            .extracting("count(n)")
            .containsExactly(6L);
    }
}
