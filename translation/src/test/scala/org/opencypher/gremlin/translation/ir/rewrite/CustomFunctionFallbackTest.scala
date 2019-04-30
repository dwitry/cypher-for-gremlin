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
package org.opencypher.gremlin.translation.ir.rewrite

import org.junit.Test
import org.opencypher.gremlin.translation.CypherAst.parse
import org.opencypher.gremlin.translation.CypherTokens._
import org.opencypher.gremlin.translation.ir.builder.IRGremlinPredicates
import org.opencypher.gremlin.translation.ir.helpers.CypherAstAssert.__
import org.opencypher.gremlin.translation.ir.helpers.CypherAstAssertions.assertThat
import org.opencypher.gremlin.translation.ir.model.{Column, CustomFunction, Scope}
import org.opencypher.gremlin.translation.translator.TranslatorFlavor

class CustomFunctionFallbackTest {
  private val flavor = TranslatorFlavor.empty //CustomFunctionFallback is applied as first rewriter
  private val P = new IRGremlinPredicates

  @Test
  def cypherPlusFallback(): Unit = {
    assertThat(parse("RETURN 1 + $noType AS a"))
      .withFlavor(flavor)
      .rewritingWith(CustomFunctionFallback)
      .removes(__.select(Column.values).map(CustomFunction.cypherPlus))
      .adds(
        __.select(Column.values)
          .local(__.unfold().choose(P.neq(NULL), __.sum())))
  }

  @Test
  def cypherSizeFallback(): Unit = {
    assertThat(parse("RETURN size($noType) AS a"))
      .withFlavor(flavor)
      .rewritingWith(CustomFunctionFallback)
      .removes(__.map(CustomFunction.cypherSize))
      .adds(__.count(Scope.local))
  }

  @Test
  def cypherPropertiesFallback(): Unit = {
    assertThat(parse("RETURN properties($noType) AS a"))
      .withFlavor(flavor)
      .rewritingWith(CustomFunctionFallback)
      .removes(__.map(CustomFunction.cypherProperties))
      .adds(
        __.local(
          __.properties()
            .group()
            .by(__.key())
            .by(__.map(__.value()))
        ))
  }

  @Test
  def cypherNodesFallback(): Unit = {
    assertThat(parse("MATCH p=()-[]->() RETURN nodes(p)"))
      .withFlavor(flavor)
      .rewritingWith(CustomFunctionFallback)
      .removes(__.is(P.isNode))
      .adds(
        __.path()
          .from(MATCH_START + "p")
          .to(MATCH_END + "p")
          .by(__.identity())
          .by(__.constant(UNUSED))
          .local(
            __.unfold()
              .is(P.neq(UNUSED))
              .fold())
      )
  }

  @Test
  def cypherRelationshipsFallback(): Unit = {
    assertThat(parse("MATCH p=()-[]->() RETURN relationships(p)"))
      .withFlavor(flavor)
      .rewritingWith(CustomFunctionFallback)
      .removes(__.is(P.isRelationship))
      .adds(
        __.path()
          .from(MATCH_START + "p")
          .to(MATCH_END + "p")
          .by(__.constant(UNUSED))
          .by(__.identity())
          .local(
            __.unfold()
              .is(P.neq(UNUSED))
              .fold())
      )
  }

}
