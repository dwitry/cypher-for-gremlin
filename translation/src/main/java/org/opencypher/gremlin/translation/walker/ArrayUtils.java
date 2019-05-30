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
package org.opencypher.gremlin.translation.walker;

import com.sun.org.apache.xpath.internal.operations.String;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ArrayUtils {
    public static boolean hasSameType(List<Object> list) {
        if (list.isEmpty()) {
            return true;
        } else {
            Class<?> clazz = list.get(0).getClass();
            return list.stream().allMatch(c -> c.getClass().equals(clazz));
        }
    }

    public static Object asArrayOfPrimitives(List<Object> list) {
        if (list.isEmpty()) {
            return new Object[]{};
        } else {
            if (Integer.class.isInstance(list.get(0))) {
                return list.stream().mapToInt(i -> ((Integer) i)).toArray();
            } else if (Long.class.isInstance(list.get(0))) {
                return list.stream().mapToLong(i -> ((Long) i)).toArray();
            } else if (Double.class.isInstance(list.get(0))) {
                return list.stream().mapToDouble(i -> ((Double) i)).toArray();
            } else if (String.class.isInstance(list.get(0))) {
                return list.toArray(new String[list.size()]);
            } else {
                return list.stream().toArray();
            }
        }
    }

    public static ArrayList<Object> asListOfObjects(Object array) {
        int length = Array.getLength(array);
        ArrayList<Object> result = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            Object e = Array.get(array, i);
            result.add(e);
        }
        return result;
    }
}
