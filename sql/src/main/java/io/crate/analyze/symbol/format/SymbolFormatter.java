/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.analyze.symbol.format;

import io.crate.analyze.symbol.Symbol;

import java.util.Locale;

import static io.crate.analyze.symbol.format.SymbolPrinter.Strings.NULL_LOWER;

public class SymbolFormatter {

    /**
     * format symbols in simple style and use the formatted symbols as {@link String#format(Locale, String, Object...)} arguments
     * for the given <code>messageTmpl</code>.
     */
    public static String format(String messageTmpl, Symbol... symbols) {
        Object[] formattedSymbols = new String[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            Symbol s = symbols[i];
            if (s == null) {
                formattedSymbols[i] = NULL_LOWER;
            } else {
                formattedSymbols[i] = SymbolPrinter.INSTANCE.printSimple(s);
            }
        }
        return String.format(Locale.ENGLISH, messageTmpl, formattedSymbols);
    }
}
