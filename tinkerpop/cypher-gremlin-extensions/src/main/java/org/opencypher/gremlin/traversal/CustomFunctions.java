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
package org.opencypher.gremlin.traversal;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.opencypher.gremlin.translation.Tokens.PROJECTION_ELEMENT;
import static org.opencypher.gremlin.translation.Tokens.PROJECTION_ID;
import static org.opencypher.gremlin.translation.Tokens.PROJECTION_INV;
import static org.opencypher.gremlin.translation.Tokens.PROJECTION_OUTV;
import static org.opencypher.gremlin.translation.Tokens.PROJECTION_RELATIONSHIP;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.opencypher.gremlin.translation.Tokens;
import org.opencypher.gremlin.translation.exception.ConstraintException;
import org.opencypher.gremlin.translation.exception.CypherExceptions;
import org.opencypher.gremlin.translation.exception.TypeException;

@SuppressWarnings({"unchecked", "WeakerAccess", "ArraysAsListWithZeroOrOneArgument"})
public final class CustomFunctions {
    private CustomFunctions() {
    }

    public static Function<Traverser, Object> cypherToString() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Boolean ||
                arg instanceof Number ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to string");
            }

            return Optional.ofNullable(arg)
                .map(String::valueOf)
                .orElse(Tokens.NULL);
        };
    }

    public static Function<Traverser, Object> cypherToBoolean() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Boolean ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to boolean");
            }

            return Optional.ofNullable(arg)
                .map(String::valueOf)
                .map(v -> {
                    switch (v.toLowerCase()) {
                        case "true":
                            return true;
                        case "false":
                            return false;
                        default:
                            return Tokens.NULL;
                    }
                })
                .orElse(Tokens.NULL);
        };
    }

    public static Function<Traverser, Object> cypherToInteger() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Number ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to integer");
            }

            return nullToToken(
                Optional.ofNullable(arg)
                    .map(String::valueOf)
                    .map(v -> {
                        try {
                            return Long.valueOf(v);
                        } catch (NumberFormatException e1) {
                            try {
                                return Double.valueOf(v).longValue();
                            } catch (NumberFormatException e2) {
                                return null;
                            }
                        }
                    })
                    .orElse(null));
        };
    }

    public static Function<Traverser, Object> cypherToFloat() {
        return traverser -> {
            Object arg = tokenToNull(traverser.get());
            boolean valid = arg == null ||
                arg instanceof Number ||
                arg instanceof String;
            if (!valid) {
                String className = arg.getClass().getName();
                throw new TypeException("Cannot convert " + className + " to float");
            }

            return nullToToken(
                Optional.ofNullable(arg)
                    .map(String::valueOf)
                    .map(v -> {
                        try {
                            return Double.valueOf(v);
                        } catch (NumberFormatException e) {
                            return null;
                        }
                    })
                    .orElse(null));
        };
    }

    public static Function<Traverser, Object> cypherProperties() {
        return traverser -> {
            Object argument = traverser.get();
            if (argument instanceof Map) {
                return argument;
            }
            Iterator<? extends Property<Object>> it = ((Element) argument).properties();
            Map<Object, Object> propertyMap = new HashMap<>();
            while (it.hasNext()) {
                Property<Object> property = it.next();
                propertyMap.putIfAbsent(property.key(), property.value());
            }
            return propertyMap;
        };
    }

    public static Function<Traverser, Object> cypherContainerIndex() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object container = args.get(0);
            Object index = args.get(1);

            if (container == Tokens.NULL || index == Tokens.NULL) {
                return Tokens.NULL;
            }

            if (container instanceof List) {
                List list = (List) container;
                int size = list.size();
                int i = normalizeContainerIndex(index, size);
                if (i < 0 || i > size) {
                    return Tokens.NULL;
                }
                return list.get(i);
            }

            if (container instanceof Map) {
                if (!(index instanceof String)) {
                    String indexClass = index.getClass().getName();
                    throw new IllegalArgumentException("Map element access by non-string: " + indexClass);
                }
                Map map = (Map) container;
                String key = (String) index;
                return map.getOrDefault(key, Tokens.NULL);
            }

            if (container instanceof Element) {
                if (!(index instanceof String)) {
                    String indexClass = index.getClass().getName();
                    throw new IllegalArgumentException("Property access by non-string: " + indexClass);
                }
                Element element = (Element) container;
                String key = (String) index;
                return element.property(key).orElse(Tokens.NULL);
            }

            String containerClass = container.getClass().getName();
            if (index instanceof String) {
                throw new IllegalArgumentException("Invalid property access of " + containerClass);
            }
            throw new IllegalArgumentException("Invalid element access of " + containerClass);
        };
    }

    public static Function<Traverser, Object> cypherListSlice() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object container = args.get(0);
            Object from = args.get(1);
            Object to = args.get(2);

            if (container == Tokens.NULL) {
                return Tokens.NULL;
            }

            if (container instanceof List) {
                List list = (List) container;
                int size = list.size();
                int f = normalizeRangeIndex(from, size);
                int t = normalizeRangeIndex(to, size);
                if (f >= t) {
                    return new ArrayList<>();
                }
                return new ArrayList<>(list.subList(f, t));
            }

            String containerClass = container.getClass().getName();
            throw new IllegalArgumentException(
                "Invalid element access of " + containerClass + " by range"
            );
        };
    }

    private static int normalizeContainerIndex(Object index, int containerSize) {
        if (!(index instanceof Number)) {
            String indexClass = index.getClass().getName();
            throw new IllegalArgumentException("List element access by non-integer: " + indexClass);
        }
        int i = ((Number) index).intValue();
        return (i >= 0) ? i : containerSize + i;
    }

    private static int normalizeRangeIndex(Object index, int size) {
        int i = normalizeContainerIndex(index, size);
        if (i < 0) {
            return 0;
        }
        if (i > size) {
            return size;
        }
        return i;
    }

    public static Function<Traverser, Object> cypherPathComprehension() {
        return traverser -> ((Collection) traverser.get()).stream()
            .map(CustomFunctions::pathToList)
            .map(path -> {
                Optional<Edge> first = ((Collection) path)
                    .stream()
                    .filter(Edge.class::isInstance)
                    .map(Edge.class::cast)
                    .findFirst();

                Edge edge = first.orElseThrow(() -> new RuntimeException("Invalid path, no edge found!"));

                Map<String, Object> result = new HashMap<>();

                Map<String, Object> projectionRelationship = new HashMap<>();
                projectionRelationship.put(PROJECTION_ID, edge.id());
                projectionRelationship.put(PROJECTION_INV, edge.inVertex().id());
                projectionRelationship.put(PROJECTION_OUTV, edge.outVertex().id());

                result.put(PROJECTION_RELATIONSHIP, asList(projectionRelationship));

                result.put(PROJECTION_ELEMENT,
                    Stream.of(
                        edge.outVertex(),
                        edge,
                        edge.inVertex())
                        .map(e -> TraversalUtil.apply(e, __.start().valueMap(true).asAdmin()))
                        .collect(toList()));

                return result;
            })
            .collect(toList());
    }

    public static Function<Traverser, Object> cypherPercentileCont() {
        return percentileFunction(
            (data, percentile) -> {
                int last = data.size() - 1;
                double lowPercentile = Math.floor(percentile * last) / last;
                double highPercentile = Math.ceil(percentile * last) / last;
                if (lowPercentile == highPercentile) {
                    return percentileNearest(data, percentile);
                }

                double scale = (percentile - lowPercentile) / (highPercentile - lowPercentile);
                double low = percentileNearest(data, lowPercentile).doubleValue();
                double high = percentileNearest(data, highPercentile).doubleValue();
                return (high - low) * scale + low;
            }
        );
    }

    public static Function<Traverser, Object> cypherPercentileDisc() {
        return percentileFunction(
            CustomFunctions::percentileNearest
        );
    }

    private static Function<Traverser, Object> percentileFunction(BiFunction<List<Number>, Double, Number> percentileStrategy) {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();

            double percentile = ((Number) args.get(1)).doubleValue();
            if (percentile < 0 || percentile > 1) {
                throw new IllegalArgumentException("Number out of range: " + percentile);
            }

            Collection<?> coll = (Collection<?>) args.get(0);
            boolean invalid = coll.stream()
                .anyMatch(o -> !(o == null || o instanceof Number));
            if (invalid) {
                throw new IllegalArgumentException("Percentile function can only handle numerical values");
            }
            List<Number> data = coll.stream()
                .filter(Objects::nonNull)
                .map(o -> (Number) o)
                .sorted()
                .collect(toList());

            int size = data.size();
            if (size == 0) {
                return Tokens.NULL;
            } else if (size == 1) {
                return data.get(0);
            }

            return percentileStrategy.apply(data, percentile);
        };
    }

    private static <T> T percentileNearest(List<T> sorted, double percentile) {
        int size = sorted.size();
        int index = (int) Math.ceil(percentile * size) - 1;
        if (index == -1) {
            index = 0;
        }
        return sorted.get(index);
    }

    public static Function<Traverser, Object> cypherSize() {
        return traverser -> traverser.get() instanceof String ?
            (long) ((String) traverser.get()).length() :
            (long) ((Collection) traverser.get()).size();
    }

    public static Function<Traverser, Object> cypherPlus() {
        return traverser -> {
            List<?> args = (List<?>) traverser.get();
            Object a = args.get(0);
            Object b = args.get(1);

            if (a == Tokens.NULL || b == Tokens.NULL) {
                return Tokens.NULL;
            }

            if (a instanceof List || b instanceof List) {
                List<Object> objects = new ArrayList<>();
                if (a instanceof List) {
                    objects.addAll((List<?>) a);
                } else {
                    objects.add(a);
                }
                if (b instanceof List) {
                    objects.addAll((List<?>) b);
                } else {
                    objects.add(b);
                }
                return objects;
            }

            if (!(a instanceof String || a instanceof Number) ||
                !(b instanceof String || b instanceof Number)) {
                throw new TypeException("Illegal use of plus operator");
            }

            if (a instanceof Number && b instanceof Number) {
                if (a instanceof Double || b instanceof Double ||
                    a instanceof Float || b instanceof Float) {
                    return ((Number) a).doubleValue() + ((Number) b).doubleValue();
                } else {
                    return ((Number) a).longValue() + ((Number) b).longValue();
                }
            } else {
                return String.valueOf(a) + String.valueOf(b);
            }
        };
    }

    public static Function<Traverser, Object> cypherException() {
        return traverser -> {
            String message = CypherExceptions.messageByName(traverser.get());
            throw new ConstraintException(message);
        };
    }

    private static Object tokenToNull(Object maybeNull) {
        return Tokens.NULL.equals(maybeNull) ? null : maybeNull;
    }

    private static Object nullToToken(Object maybeNull) {
        return maybeNull == null ? Tokens.NULL : maybeNull;
    }

    private static Object pathToList(Object value) {
        return value instanceof Path ? new ArrayList<>(((Path) value).objects()) : value;
    }
}
