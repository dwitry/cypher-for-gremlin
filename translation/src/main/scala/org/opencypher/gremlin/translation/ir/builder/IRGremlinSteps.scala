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

import org.opencypher.gremlin.translation.GremlinSteps
import org.opencypher.gremlin.translation.ir.model._
import org.opencypher.gremlin.traversal.CustomFunction

import scala.collection.mutable

class IRGremlinSteps extends GremlinSteps[Seq[GremlinStep], IRGremlinTokens] {

  private val buf = mutable.ListBuffer.empty[GremlinStep]

  override def current(): Seq[GremlinStep] = buf.toList

  override def start(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = new IRGremlinSteps

  override def V(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Vertex
    this
  }

  override def E(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Edge
    this
  }

  override def addE(edgeLabel: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += AddE(edgeLabel)
    this
  }

  override def addV(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += AddV
    this
  }

  override def addV(vertexLabel: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += AddV(vertexLabel)
    this
  }

  override def aggregate(sideEffectKey: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Aggregate(sideEffectKey)
    this
  }

  override def and(andTraversals: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]*)
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += And(andTraversals.map(_.current): _*)
    this
  }

  override def as(stepLabel: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += As(stepLabel)
    this
  }

  override def barrier(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Barrier
    this
  }

  override def bothE(edgeLabels: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += BothE(edgeLabels: _*)
    this
  }

  override def by(
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += By(traversal.current(), None)
    this
  }

  override def by(
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens],
      order: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += By(traversal.current(), Some(order.getOrder))
    this
  }

  override def cap(sideEffectKey: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Cap(sideEffectKey)
    this
  }

  override def choose(choiceTraversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += ChooseT1(choiceTraversal.current())
    this
  }

  override def choose(
      traversalPredicate: GremlinSteps[Seq[GremlinStep], IRGremlinTokens],
      trueChoice: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += ChooseT2(traversalPredicate.current(), trueChoice.current())
    this
  }

  override def choose(
      traversalPredicate: GremlinSteps[Seq[GremlinStep], IRGremlinTokens],
      trueChoice: GremlinSteps[Seq[GremlinStep], IRGremlinTokens],
      falseChoice: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += ChooseT3(traversalPredicate.current(), trueChoice.current(), falseChoice.current())
    this
  }

  override def choose(
      predicate: IRGremlinTokens,
      trueChoice: GremlinSteps[Seq[GremlinStep], IRGremlinTokens],
      falseChoice: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += ChooseP3(predicate.getPredicate, trueChoice.current(), falseChoice.current())
    this
  }

  override def choose(
      predicate: IRGremlinTokens,
      trueChoice: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += ChooseP2(predicate.getPredicate, trueChoice.current())
    this
  }

  override def coalesce(coalesceTraversals: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]*)
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Coalesce(coalesceTraversals.map(_.current): _*)
    this
  }

  override def constant(e: Any): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Constant(e)
    this
  }

  override def count(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Count
    this
  }

  override def count(scope: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += CountS(scope.getScope)
    this
  }

  override def dedup(dedupLabels: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Dedup(dedupLabels: _*)
    this
  }

  override def drop(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Drop
    this
  }

  override def emit(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Emit
    this
  }

  override def emit(
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += EmitT(traversal.current())
    this
  }

  override def flatMap(
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += FlatMapT(traversal.current())
    this
  }

  override def fold(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Fold
    this
  }

  override def from(fromStepLabel: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += From(fromStepLabel)
    this
  }

  override def group(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Group
    this
  }

  override def has(propertyKey: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Has(propertyKey)
    this
  }

  override def has(propertyKey: String, predicate: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += HasP(propertyKey, predicate.getPredicate)
    this
  }

  override def hasKey(labels: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += HasKey(labels: _*)
    this
  }

  override def hasLabel(labels: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += HasLabel(labels: _*)
    this
  }

  override def hasNot(propertyKey: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += HasNot(propertyKey)
    this
  }

  override def id(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Id
    this
  }

  override def identity(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Identity
    this
  }

  override def inE(edgeLabels: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += InE(edgeLabels: _*)
    this
  }

  override def inV(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += InV
    this
  }

  override def index(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Index
    this
  }

  override def inject(injections: AnyRef*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Inject(injections: _*)
    this
  }

  override def is(predicate: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Is(predicate.getPredicate)
    this
  }

  override def key(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Key
    this
  }

  override def label(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Label
    this
  }

  override def limit(limit: Long): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Limit(limit)
    this
  }

  override def limit(scope: IRGremlinTokens, limit: Long): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += LimitS(scope.getScope, limit)
    this
  }

  override def local(
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Local(traversal.current())
    this
  }

  override def loops(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Loops
    this
  }

  override def map(function: CustomFunction): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += MapF(function)
    this
  }

  override def map(
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += MapT(traversal.current())
    this
  }

  override def math(expression: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Math(expression)
    this
  }

  override def max(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Max
    this
  }

  override def max(scope: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += MaxS(scope.getScope)
    this
  }

  override def mean(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Mean
    this
  }

  override def mean(scope: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += MeanS(scope.getScope)
    this
  }

  override def min(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Min
    this
  }

  override def min(scope: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += MinS(scope.getScope)
    this
  }

  override def not(notTraversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Not(notTraversal.current())
    this
  }

  override def option(pickToken: Object, traversalOption: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += OptionT(pickToken, traversalOption.current())
    this
  }

  override def optional(optionalTraversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Optional(optionalTraversal.current())
    this
  }

  override def or(orTraversals: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]*)
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Or(orTraversals.map(_.current): _*)
    this
  }

  override def order(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Order
    this
  }

  override def otherV(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += OtherV
    this
  }

  override def outE(edgeLabels: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += OutE(edgeLabels: _*)
    this
  }

  override def outV(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += OutV
    this
  }

  override def path(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Path
    this
  }

  override def properties(propertyKeys: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Properties(propertyKeys: _*)
    this
  }

  override def property(key: IRGremlinTokens, value: Any): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += PropertyG(key.getToken, value)
    this
  }

  override def property(key: String, value: Any): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += PropertyV(key, value)
    this
  }

  override def property(
      cardinality: IRGremlinTokens,
      key: String,
      value: Any): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += PropertyVC(cardinality.getCardinality, key, value)
    this
  }

  override def property(
      key: String,
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += PropertyT(key, traversal.current())
    this
  }

  override def property(
      cardinality: IRGremlinTokens,
      key: String,
      traversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += PropertyTC(cardinality.getCardinality, key, traversal.current())
    this
  }

  override def project(keys: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Project(keys: _*)
    this
  }

  override def range(scope: IRGremlinTokens, low: Long, high: Long): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Range(scope.getScope, low, high)
    this
  }

  override def repeat(repeatTraversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Repeat(repeatTraversal.current())
    this
  }

  override def select(pop: IRGremlinTokens, selectKey: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += SelectP(pop.getPop, selectKey)
    this
  }

  override def select(selectKeys: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += SelectK(selectKeys: _*)
    this
  }

  override def select(column: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += SelectC(column.getColumn)
    this
  }

  override def sideEffect(sideEffectTraversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += SideEffect(sideEffectTraversal.current())
    this
  }

  override def simplePath(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += SimplePath
    this
  }

  override def skip(skip: Long): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Skip(skip)
    this
  }

  override def sum(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Sum
    this
  }

  override def sum(scope: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += SumS(scope.getScope)
    this
  }

  override def tail(scope: IRGremlinTokens, limit: Long): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Tail(scope.getScope, limit)
    this
  }

  override def times(maxLoops: Int): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Times(maxLoops)
    this
  }

  override def to(toStepLabel: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += To(toStepLabel)
    this
  }

  override def unfold(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Unfold
    this
  }

  override def union(unionTraversals: GremlinSteps[Seq[GremlinStep], IRGremlinTokens]*)
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Union(unionTraversals.map(_.current): _*)
    this
  }

  override def until(untilTraversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Until(untilTraversal.current())
    this
  }

  override def value(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Value
    this
  }

  override def valueMap(): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += ValueMap
    this
  }

  override def valueMap(includeTokens: Boolean): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += ValueMap(includeTokens)
    this
  }

  override def values(propertyKeys: String*): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += Values(propertyKeys: _*)
    this
  }

  override def where(whereTraversal: GremlinSteps[Seq[GremlinStep], IRGremlinTokens])
    : GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += WhereT(whereTraversal.current())
    this
  }

  override def where(predicate: IRGremlinTokens): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += WhereP(predicate.getPredicate)
    this
  }

  override def `with`(key: String): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += WithK(key)
    this
  }

  override def `with`(name: String, value: Object): GremlinSteps[Seq[GremlinStep], IRGremlinTokens] = {
    buf += With(name, value)
    this
  }
}
