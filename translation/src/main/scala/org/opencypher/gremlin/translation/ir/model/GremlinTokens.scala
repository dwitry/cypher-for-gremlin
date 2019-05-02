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
package org.opencypher.gremlin.translation.ir.model

trait GremlinToken
object GremlinToken {
  val id = "id"
}

trait Scope extends GremlinToken
object Scope {
  object global extends Scope
  object local extends Scope
}

trait Column extends GremlinToken
object Column {
  object values extends Column
  object keys extends Column
}

trait TraversalOrder extends GremlinToken
object TraversalOrder {
  object asc extends TraversalOrder
  object desc extends TraversalOrder
  object incr extends TraversalOrder
  object decr extends TraversalOrder
}

trait WithOptions extends GremlinToken
object WithOptions {
  val tokens = "tokens"
}

trait Pop extends GremlinToken
object Pop {
  object all extends Pop
}

trait Cardinality extends GremlinToken
object Cardinality {
  object single extends Cardinality
}

object Vertex2 extends GremlinToken {
  val DEFAULT_LABEL = "vertex"
  val none = "none" //pick

}
