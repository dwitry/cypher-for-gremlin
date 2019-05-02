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
package org.opencypher.gremlin.translation.ir.builder
import org.opencypher.gremlin.translation.ir.model._
import org.opencypher.gremlin.translation.{GremlinPredicates, GremlinTokens}
import org.opencypher.gremlin.traversal.CustomFunction

class IRGremlinTokens
    extends GremlinTokens[
      GremlinPredicate,
      CustomFunction,
      Scope,
      Column,
      TraversalOrder,
      WithOptions,
      Pop,
      Cardinality,
      GremlinToken] {
  override def local(): GremlinTokens.GremlinToken[
    GremlinPredicate,
    CustomFunction,
    Scope,
    Column,
    TraversalOrder,
    WithOptions,
    Pop,
    Cardinality,
    org.opencypher.gremlin.translation.ir.model.GremlinToken] = scope(Scope.local)

  override def global(): GremlinTokens.GremlinToken[
    GremlinPredicate,
    CustomFunction,
    Scope,
    Column,
    TraversalOrder,
    WithOptions,
    Pop,
    Cardinality,
    org.opencypher.gremlin.translation.ir.model.GremlinToken] = scope(Scope.global)

  override def values(): GremlinTokens.GremlinToken[
    GremlinPredicate,
    CustomFunction,
    Scope,
    Column,
    TraversalOrder,
    WithOptions,
    Pop,
    Cardinality,
    org.opencypher.gremlin.translation.ir.model.GremlinToken] = column(Column.values)

  override def keys(): GremlinTokens.GremlinToken[
    GremlinPredicate,
    CustomFunction,
    Scope,
    Column,
    TraversalOrder,
    WithOptions,
    Pop,
    Cardinality,
    org.opencypher.gremlin.translation.ir.model.GremlinToken] = column(Column.keys)

  override def predicates(): GremlinPredicates[GremlinPredicate] = new IRGremlinPredicates()
}
