/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.lang.common.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.statement.FunctionDecl;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.metadata.entities.Function;

public class FunctionParser {

    private final Function.FunctionLanguage language;

    private final IParserFactory parserFactory;

    public FunctionParser(Function.FunctionLanguage language, IParserFactory parserFactory) {
        this.language = language;
        this.parserFactory = parserFactory;
    }

    public FunctionDecl getFunctionDecl(Function function) throws CompilationException {
        if (!function.getLanguage().equals(language)) {
            throw new CompilationException(ErrorCode.COMPILATION_INCOMPATIBLE_FUNCTION_LANGUAGE, language,
                    function.getLanguage());
        }

        FunctionSignature signature = function.getSignature();

        List<String> argNames = function.getArgNames();
        List<VarIdentifier> paramList = new ArrayList<>(argNames.size());
        for (String argName : argNames) {
            paramList.add(new VarIdentifier(argName));
        }

        String functionBody = function.getFunctionBody();
        IParser parser = parserFactory.createParser(new StringReader(functionBody));
        Expression functionBodyExpr = parser.parseFunctionBody(signature, paramList);

        return new FunctionDecl(signature, paramList, functionBodyExpr);
    }
}