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
package org.opencypher.gremlin.translation;

import org.opencypher.gremlin.translation.ir.model.Cardinality;
import org.opencypher.gremlin.translation.ir.model.Column;
import org.opencypher.gremlin.translation.ir.model.CustomFunction;
import org.opencypher.gremlin.translation.ir.model.Pick;
import org.opencypher.gremlin.translation.ir.model.Pop;
import org.opencypher.gremlin.translation.ir.model.Scope;
import org.opencypher.gremlin.translation.ir.model.TraversalOrder;

public interface TokensConverter<PR, FN, SC, CO, OR, WI, PO, CA, TO> {
    PO convert(Pop pop);

    SC convert(Scope scope);

    CA convert(Cardinality scope);

    CO convert(Column scope);

    OR convert(TraversalOrder scope);

    FN convert(CustomFunction function);

    TO convert(Pick pickToken);
}
