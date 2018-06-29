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
package org.opencypher.gremlin.translation.ir.rewrite

import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single
import org.junit.Test
import org.opencypher.gremlin.translation.CypherAst.parse
import org.opencypher.gremlin.translation.ir.helpers.CypherAstAssert.__
import org.opencypher.gremlin.translation.ir.helpers.CypherAstAssertions.assertThat
import org.opencypher.gremlin.translation.ir.helpers.JavaHelpers.objects
import org.opencypher.gremlin.translation.translator.TranslatorFlavor

class NeptuneFlavorTest {

  val flavor = TranslatorFlavor.gremlinServer

  @Test
  def vertexCardinality(): Unit = {
    assertThat(parse("CREATE (:A {name: 'Andres'})"))
      .withFlavor(flavor)
      .rewritingWith(NeptuneFlavor)
      .removes(__.property("name", "Andres"))
      .adds(__.property(single, "name", "Andres"))
  }

  @Test
  def vertexCardinalityMultiple(): Unit = {
    assertThat(parse("CREATE (:X {foo: 'A', bar: 'B'})"))
      .withFlavor(flavor)
      .rewritingWith(NeptuneFlavor)
      .removes(__.property("foo", "A"))
      .removes(__.property("bar", "B"))
      .adds(__.property(single, "foo", "A"))
      .adds(__.property(single, "bar", "B"))
  }

  @Test
  def vertexCardinalityNested(): Unit = {
    assertThat(parse("""
        |MATCH (n:X {foo: 'A'})
        |SET n += {bar: 'C'}
        |RETURN n
      """.stripMargin))
      .withFlavor(flavor)
      .rewritingWith(NeptuneFlavor)
      .removes(__.property("bar", "C"))
      .adds(__.property(single, "bar", "C"))
  }

  @Test
  def ignoreEdgeCardinality(): Unit = {
    assertThat(parse("""
        |CREATE (a {vp1: 'vp'}),
        |(b {vp2: 'vp'}),
        |(a)-[:X {prop: 42, a: 'a', b: 'B'}]->(b)""".stripMargin))
      .withFlavor(flavor)
      .rewritingWith(NeptuneFlavor)
      .keeps(__.property("prop", 42L))
      .keeps(__.property("a", "a"))
      .keeps(__.property("b", "B"))
      .removes(__.property("vp1", "vp"))
      .removes(__.property("vp2", "vp"))
      .adds(__.property(single, "vp1", "vp"))
      .adds(__.property(single, "vp2", "vp"))
  }
}
