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

import org.opencypher.gremlin.translation.CypherTokens._
import org.opencypher.gremlin.translation.exception.CypherExceptions
import org.opencypher.gremlin.translation.ir.TraversalHelper._
import org.opencypher.gremlin.translation.ir.model.Scope.local
import org.opencypher.gremlin.translation.ir.model._

/**
  * Replaces Custom Functions with "The Best We Could Do" Gremlin native alternatives
  */
object CustomFunctionFallback extends GremlinRewriter {
  def prepend(rewriters: Seq[GremlinRewriter]): Seq[GremlinRewriter] = {
    CustomFunctionFallback +: rewriters
  }

  override def apply(steps: Seq[GremlinStep]): Seq[GremlinStep] = {

    mapTraversals(replace({
      case Constant(typ) :: MapC(CustomFunction.cypherException) :: rest =>
        val text = CypherExceptions.messageByName(typ)
        Path :: From(text) :: rest

      case SelectC(values) :: MapC(CustomFunction.cypherPlus) :: rest =>
        SelectC(values) :: Local(Unfold :: ChooseP2(Neq(NULL), Sum :: Nil) :: Nil) :: rest

      case MapC(CustomFunction.cypherSize) :: rest =>
        CountS(local) :: rest

      case MapC(CustomFunction.cypherProperties) :: rest =>
        Local(Properties() :: Group :: By(Key :: Nil, None) :: By(MapT(Value :: Nil) :: Nil, None) :: Nil) :: rest

      case SelectK(pathName) :: FlatMapT(MapT(Unfold :: Is(IsNode()) :: Fold :: Nil) :: Nil) :: rest =>
        SelectK(pathName) :: Path :: From(MATCH_START + pathName) :: To(MATCH_END + pathName) :: By(Identity :: Nil) :: By(
          Constant(UNUSED) :: Nil) :: Local(Unfold :: Is(Neq(UNUSED)) :: Fold :: Nil) :: rest

      case SelectK(pathName) :: FlatMapT(MapT(Unfold :: Is(IsRelationship()) :: Fold :: Nil) :: Nil) :: rest =>
        SelectK(pathName) :: Path :: From(MATCH_START + pathName) :: To(MATCH_END + pathName) :: By(
          Constant(UNUSED) :: Nil) :: By(Identity :: Nil) :: Local(Unfold :: Is(Neq(UNUSED)) :: Fold :: Nil) :: rest
    }))(steps)
  }
}
