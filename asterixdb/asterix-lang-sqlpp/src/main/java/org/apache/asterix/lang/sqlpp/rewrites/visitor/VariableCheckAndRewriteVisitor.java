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
package org.apache.asterix.lang.sqlpp.rewrites.visitor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DatasetFullyQualifiedName;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Expression.Kind;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.expression.FieldAccessor;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.rewrites.LangRewritingContext;
import org.apache.asterix.lang.common.statement.DatasetDecl;
import org.apache.asterix.lang.common.statement.ViewDecl;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.expression.WindowExpression;
import org.apache.asterix.lang.sqlpp.util.FunctionMapUtil;
import org.apache.asterix.lang.sqlpp.util.SqlppVariableUtil;
import org.apache.asterix.lang.sqlpp.visitor.CheckDatasetOnlyResolutionVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppExpressionScopingVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class VariableCheckAndRewriteVisitor extends AbstractSqlppExpressionScopingVisitor {

    protected final MetadataProvider metadataProvider;

    /**
     * @param context,
     *            manages ids of variables and guarantees uniqueness of variables.
     */
    public VariableCheckAndRewriteVisitor(LangRewritingContext context, MetadataProvider metadataProvider,
            Collection<VarIdentifier> externalVars) {
        super(context, externalVars);
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Expression visit(VariableExpr varExpr, ILangExpression parent) throws CompilationException {
        if (resolveAsVariableReference(varExpr)) {
            return varExpr;
        }
        Namespace defaultNamespace = metadataProvider.getDefaultNamespace();
        DataverseName dataverseName = defaultNamespace.getDataverseName();
        String databaseName = defaultNamespace.getDatabaseName();
        String datasetName = SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue();
        CallExpr datasetExpr = resolveAsDataset(databaseName, dataverseName, datasetName, parent, varExpr);
        return datasetExpr != null ? datasetExpr : resolveAsFieldAccessOverContextVar(varExpr);
    }

    @Override
    public Expression visit(FieldAccessor fa, ILangExpression parent) throws CompilationException {
        Expression leadingExpr = fa.getExpr();
        if (leadingExpr.getKind() == Kind.VARIABLE_EXPRESSION) {
            // resolving a.b
            VariableExpr leadingVarExpr = (VariableExpr) leadingExpr;
            if (resolveAsVariableReference(leadingVarExpr)) {
                return fa;
            } else {
                String dataverseNamePart =
                        SqlppVariableUtil.toUserDefinedVariableName(leadingVarExpr.getVar().getValue()).getValue();
                DataverseName dataverseName; // 1-part name
                try {
                    dataverseName = DataverseName.createSinglePartName(dataverseNamePart);
                } catch (AsterixException e) {
                    throw new CompilationException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, fa.getSourceLocation(),
                            dataverseNamePart);
                }
                //TODO(DB): decide
                String databaseName = MetadataUtil.databaseFor(dataverseName);
                String datasetName = fa.getIdent().getValue();
                CallExpr datasetExpr =
                        resolveAsDataset(databaseName, dataverseName, datasetName, parent, leadingVarExpr);
                if (datasetExpr != null) {
                    return datasetExpr;
                } else {
                    fa.setExpr(resolveAsFieldAccessOverContextVar(leadingVarExpr));
                    return fa;
                }
            }
        } else {
            List<String> dataverseNameParts = new ArrayList<>(4);
            Pair<VariableExpr, FieldAccessor> topExprs = new Pair<>(null, null);
            if (extractDataverseName(fa.getExpr(), dataverseNameParts, topExprs)) {
                // resolving a.b.c(.x)*
                VariableExpr topVarExpr = topExprs.getFirst(); // = a
                if (resolveAsVariableReference(topVarExpr)) {
                    return fa;
                } else {
                    Namespace namespace;
                    try {
                        namespace = metadataProvider.resolve(dataverseNameParts);
                    } catch (AsterixException e) {
                        throw new CompilationException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, fa.getSourceLocation(),
                                dataverseNameParts.toString());
                    }
                    //TODO(DB): decide
                    String databaseName = namespace.getDatabaseName();
                    DataverseName dataverseName = namespace.getDataverseName();
                    String datasetName = fa.getIdent().getValue();
                    CallExpr datasetExpr =
                            resolveAsDataset(databaseName, dataverseName, datasetName, parent, topVarExpr);
                    if (datasetExpr != null) {
                        return datasetExpr;
                    }
                    FieldAccessor topFaExpr = topExprs.getSecond(); // = a.b
                    topFaExpr.setExpr(resolveAsFieldAccessOverContextVar(topVarExpr));
                    return fa;
                }
            } else {
                fa.setExpr(leadingExpr.accept(this, parent));
                return fa;
            }
        }
    }

    private boolean resolveAsVariableReference(VariableExpr varExpr) throws CompilationException {
        VarIdentifier varId = varExpr.getVar();
        String varName = varId.getValue();
        if (scopeChecker.isInForbiddenScopes(varName)) {
            throw new CompilationException(ErrorCode.FORBIDDEN_SCOPE, varExpr.getSourceLocation());
        }
        Identifier ident = scopeChecker.lookupSymbol(varName);
        if (ident == null) {
            if (SqlppVariableUtil.isExternalVariableIdentifier(varId)) {
                throw new CompilationException(ErrorCode.PARAMETER_NO_VALUE, varExpr.getSourceLocation(),
                        SqlppVariableUtil.variableNameToDisplayedFieldName(varId.getValue()));
            } else {
                return false;
            }
        }
        // Exists such an identifier
        varExpr.setIsNewVar(false);
        varExpr.setVar((VarIdentifier) ident);
        return true;
    }

    // try resolving the undefined identifier reference as a dataset access.
    // for a From/Join/UNNEST/Quantifiers binding expression
    private CallExpr resolveAsDataset(String databaseName, DataverseName dataverseName, String datasetName,
            ILangExpression parent, VariableExpr varExpr) throws CompilationException {
        if (!parent.accept(CheckDatasetOnlyResolutionVisitor.INSTANCE, varExpr)) {
            return null;
        }
        SourceLocation sourceLoc = varExpr.getSourceLocation();
        String resolvedDatabaseName;
        DataverseName resolvedDataverseName;
        String resolvedDatasetName;
        boolean viaSynonym, isView;
        ViewDecl viewDecl = findDeclaredView(databaseName, dataverseName, datasetName);
        DatasetDecl datasetDecl = findDeclaredDataset(databaseName, dataverseName, datasetName);
        if (viewDecl != null) {
            resolvedDatabaseName = viewDecl.getViewName().getDatabaseName();
            resolvedDataverseName = viewDecl.getViewName().getDataverseName();
            resolvedDatasetName = viewDecl.getViewName().getDatasetName();
            viaSynonym = false;
            isView = true;
        } else if (datasetDecl != null) {
            resolvedDatabaseName = databaseName;
            resolvedDataverseName = dataverseName;
            resolvedDatasetName = datasetName;
            viaSynonym = false;
            isView = false;
        } else {
            Pair<Dataset, Boolean> p = findDataset(databaseName, dataverseName, datasetName, true, sourceLoc);
            if (p == null) {
                throw createUnresolvableError(databaseName, dataverseName, datasetName, sourceLoc);
            }
            Dataset resolvedDataset = p.first;
            resolvedDatabaseName = resolvedDataset.getDatabaseName();
            resolvedDataverseName = resolvedDataset.getDataverseName();
            resolvedDatasetName = resolvedDataset.getDatasetName();
            viaSynonym = p.second;
            isView = resolvedDataset.getDatasetType() == DatasetConfig.DatasetType.VIEW;
        }
        CallExpr callExpr;
        if (viaSynonym) {
            callExpr = FunctionUtil.makeSynonymDatasetCallExpr(resolvedDatabaseName, resolvedDataverseName,
                    resolvedDatasetName, isView, databaseName, dataverseName, datasetName);
        } else {
            callExpr = FunctionUtil.makeDatasetCallExpr(resolvedDatabaseName, resolvedDataverseName,
                    resolvedDatasetName, isView);
        }
        callExpr.addHints(varExpr.getHints());
        callExpr.setSourceLocation(sourceLoc);
        return callExpr;
    }

    // resolve the undefined identifier reference as a field access on a context variable
    private FieldAccessor resolveAsFieldAccessOverContextVar(VariableExpr varExpr) throws CompilationException {
        Map<VariableExpr, Set<? extends Scope.SymbolAnnotation>> localVars =
                scopeChecker.getCurrentScope().getLiveVariables(scopeChecker.getPrecedingScope());
        Set<VariableExpr> contextVars = Scope.findVariablesAnnotatedBy(localVars.keySet(),
                SqlppVariableAnnotation.CONTEXT_VARIABLE, localVars, varExpr.getSourceLocation());
        VariableExpr contextVar = pickContextVar(contextVars, varExpr);
        return generateFieldAccess(contextVar, varExpr.getVar(), varExpr.getSourceLocation());
    }

    // Rewrites for a field access by name
    static FieldAccessor generateFieldAccess(Expression sourceExpr, VarIdentifier fieldVar, SourceLocation sourceLoc) {
        VarIdentifier fieldName = SqlppVariableUtil.toUserDefinedVariableName(fieldVar.getValue());
        FieldAccessor fa = new FieldAccessor(sourceExpr, fieldName);
        fa.setSourceLocation(sourceLoc);
        return fa;
    }

    private static boolean extractDataverseName(Expression expr, List<String> outDataverseName,
            Pair<VariableExpr, FieldAccessor> outTopExprs) {
        switch (expr.getKind()) {
            case VARIABLE_EXPRESSION:
                VariableExpr varExpr = (VariableExpr) expr;
                String varName = SqlppVariableUtil.toUserDefinedVariableName(varExpr.getVar().getValue()).getValue();
                outDataverseName.add(varName);
                outTopExprs.setFirst(varExpr);
                return true;
            case FIELD_ACCESSOR_EXPRESSION:
                FieldAccessor faExpr = (FieldAccessor) expr;
                if (extractDataverseName(faExpr.getExpr(), outDataverseName, outTopExprs)) {
                    outDataverseName.add(faExpr.getIdent().getValue());
                    if (outTopExprs.getSecond() == null) {
                        outTopExprs.setSecond(faExpr);
                    }
                    return true;
                } else {
                    return false;
                }
            default:
                return false;
        }
    }

    private CompilationException createUnresolvableError(String databaseName, DataverseName dataverseName,
            String datasetName, SourceLocation sourceLoc) {
        DataverseName defaultDataverseName = metadataProvider.getDefaultNamespace().getDataverseName();
        String defaultDatabaseName = metadataProvider.getDefaultNamespace().getDatabaseName();
        if (dataverseName == null && defaultDataverseName == null) {
            return new CompilationException(ErrorCode.NAME_RESOLVE_UNKNOWN_DATASET, sourceLoc, datasetName);
        }
        boolean useDb = metadataProvider.isUsingDatabase();
        String namespace;
        if (dataverseName == null) {
            namespace = MetadataUtil.dataverseName(defaultDatabaseName, defaultDataverseName, useDb);
        } else {
            namespace = MetadataUtil.dataverseName(databaseName, dataverseName, useDb);
        }
        //If no available dataset nor in-scope variable to resolve to, we throw an error.
        return new CompilationException(ErrorCode.NAME_RESOLVE_UNKNOWN_DATASET_IN_DATAVERSE, sourceLoc, datasetName,
                namespace);
    }

    private Pair<Dataset, Boolean> findDataset(String databaseName, DataverseName dataverseName, String datasetName,
            boolean includingViews, SourceLocation sourceLoc) throws CompilationException {
        try {
            Boolean viaSynonym = false;
            Quadruple<DataverseName, String, Boolean, String> dsName = metadataProvider
                    .resolveDatasetNameUsingSynonyms(databaseName, dataverseName, datasetName, includingViews);
            if (dsName != null) {
                dataverseName = dsName.getFirst();
                databaseName = dsName.getFourth();
                datasetName = dsName.getSecond();
                viaSynonym = dsName.getThird();
            }
            Dataset dataset = metadataProvider.findDataset(databaseName, dataverseName, datasetName, includingViews);
            return dataset == null ? null : new Pair<>(dataset, viaSynonym);
        } catch (AlgebricksException e) {
            throw new CompilationException(ErrorCode.COMPILATION_ERROR, e, sourceLoc, e.getMessage());
        }
    }

    private ViewDecl findDeclaredView(String databaseName, DataverseName dataverseName, String viewName) {
        Map<DatasetFullyQualifiedName, ViewDecl> declaredViews = context.getDeclaredViews();
        return declaredViews.isEmpty() ? null
                : declaredViews.get(new DatasetFullyQualifiedName(databaseName, dataverseName, viewName));
    }

    private DatasetDecl findDeclaredDataset(String databaseName, DataverseName dataverseName, String datasetName) {
        Map<DatasetFullyQualifiedName, DatasetDecl> declaredDatasets = context.getDeclaredDatasets();
        return declaredDatasets.isEmpty() ? null
                : declaredDatasets.get(new DatasetFullyQualifiedName(databaseName, dataverseName, datasetName));
    }

    @Override
    public Expression visit(CallExpr callExpr, ILangExpression arg) throws CompilationException {
        // skip variables inside SQL-92 aggregates (they will be resolved by SqlppGroupByAggregationSugarVisitor)
        if (FunctionMapUtil.isSql92AggregateFunction(callExpr.getFunctionSignature())) {
            return callExpr;
        }
        return super.visit(callExpr, arg);
    }

    @Override
    public Expression visit(WindowExpression winExpr, ILangExpression arg) throws CompilationException {
        // skip variables inside list and agg-filter arguments of window functions
        // (will be resolved by SqlppWindowExpressionVisitor)
        FunctionSignature fs = winExpr.getFunctionSignature();
        FunctionIdentifier winfi = FunctionMapUtil.getInternalWindowFunction(fs);
        if (winfi != null) {
            if (BuiltinFunctions.builtinFunctionHasProperty(winfi,
                    BuiltinFunctions.WindowFunctionProperty.HAS_LIST_ARG)) {
                visitWindowExpressionExcludingExprListAndAggFilter(winExpr, arg);
                List<Expression> exprList = winExpr.getExprList();
                List<Expression> newExprList = new ArrayList<>(exprList.size());
                Iterator<Expression> i = exprList.iterator();
                newExprList.add(i.next()); // don't visit the list arg
                while (i.hasNext()) {
                    newExprList.add(visit(i.next(), arg));
                }
                winExpr.setExprList(newExprList);
                return winExpr;
            } else {
                return super.visit(winExpr, arg);
            }
        } else if (FunctionMapUtil.isSql92AggregateFunction(fs)) {
            visitWindowExpressionExcludingExprListAndAggFilter(winExpr, arg);
            return winExpr;
        } else {
            return super.visit(winExpr, arg);
        }
    }

    static VariableExpr pickContextVar(Collection<VariableExpr> contextVars, VariableExpr usedVar)
            throws CompilationException {
        switch (contextVars.size()) {
            case 0:
                throw new CompilationException(ErrorCode.UNDEFINED_IDENTIFIER, usedVar.getSourceLocation(),
                        SqlppVariableUtil.toUserDefinedVariableName(usedVar.getVar().getValue()).getValue());
            case 1:
                return contextVars.iterator().next();
            default:
                throw new CompilationException(ErrorCode.AMBIGUOUS_IDENTIFIER, usedVar.getSourceLocation(),
                        SqlppVariableUtil.toUserDefinedVariableName(usedVar.getVar().getValue()).getValue());
        }
    }
}
