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
package org.opencypher.gremlin.translation.ir

import org.junit.Test
import org.opencypher.gremlin.translation.CypherAst.parse
import org.opencypher.gremlin.translation.Tokens._
import org.opencypher.gremlin.translation.ir.helpers.CypherAstAssert._
import org.opencypher.gremlin.translation.ir.helpers.CypherAstAssertions.assertThat
import org.opencypher.gremlin.translation.translator.TranslatorFlavor

class NullGuardTest {

  @Test
  def singleProjection(): Unit = {
    assertThat(parse("""
        |MATCH (n)
        |RETURN n
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .doesNotContain(__.choose(P.neq(NULL), __.valueMap(true)))
  }

  @Test
  def singleOptionalProjection(): Unit = {
    assertThat(parse("""
        |OPTIONAL MATCH (n)
        |RETURN n
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .contains(__.choose(P.neq(NULL), __.valueMap(true)))
  }

  @Test
  def functionInvocation(): Unit = {
    assertThat(parse("""
      MATCH (n:notExising) WITH n AS n RETURN head(collect(n)) AS head
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .contains(__.choose(P.neq(NULL), __.valueMap(true)))
  }

  @Test
  def optionalProjections(): Unit = {
    assertThat(parse("""
        |MATCH (n)-->(m)
        |RETURN n, m
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .doesNotContain(__.choose(P.neq(NULL), __.valueMap(true)))
  }

  @Test
  def multipleOptionalProjections(): Unit = {
    assertThat(parse("""
        |OPTIONAL MATCH (n)-->(m)
        |RETURN n, m
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .contains(__.choose(P.neq(NULL), __.valueMap(true)))
  }
  @Test
  def valueProjection(): Unit = {
    assertThat(parse("""
        |MATCH (a)-[r {name: 'r'}]-(b) RETURN a.value, b.value
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .contains(__.constant("dsf"))
      .doesNotContain(
        __.select("a").choose(P.neq(NULL), __.choose(__.values("value"), __.values("value"), __.constant(NULL))))
      .doesNotContain(
        __.select("b").choose(P.neq(NULL), __.choose(__.values("value"), __.values("value"), __.constant(NULL))))
  }

  @Test
  def optionalValueProjection(): Unit = {
    assertThat(parse("""
        |OPTIONAL MATCH (a)-[r {name: 'r'}]-(b) RETURN a.value, b.value
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .contains(
        __.select("a").choose(P.neq(NULL), __.choose(__.values("value"), __.values("value"), __.constant(NULL))))
      .contains(
        __.select("b").choose(P.neq(NULL), __.choose(__.values("value"), __.values("value"), __.constant(NULL))))
  }

  @Test
  def optionalWithProjection(): Unit = {
    assertThat(parse("""
        |OPTIONAL MATCH (n:notExisting) WITH (n) as m RETURN m
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .contains(__.choose(P.neq(NULL), __.valueMap(true)))
  }

  @Test
  def create(): Unit = {
    assertThat(parse("""
        |CREATE (n)-[r:knows]->(m) RETURN n, r, m
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .doesNotContain(__.choose(P.neq(NULL), __.valueMap(true)))
  }

  @Test
  def merge(): Unit = {
    assertThat(parse("""
        |MERGE (n)-[r:knows]->(m) RETURN n, r, m
      """.stripMargin))
      .withFlavor(TranslatorFlavor.gremlinServer)
      .doesNotContain(__.choose(P.neq(NULL), __.valueMap(true)))
  }
}
